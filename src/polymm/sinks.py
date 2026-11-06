"""Sinks: RawSink (verbatim frames, zstd) and ParquetSink (typed tables).

Both buffer in memory and flush in one call that the recorder runs in a worker
thread, so the event loop never waits on compression or fsync.

Why atomic rename: write to `name.tmp`, then `os.replace` to `name`. A crash
mid-write leaves a `.tmp` (ignored by readers), never a corrupt Parquet file.

Layout (decision 2):
  data/raw/ws_market/dt=YYYY-MM-DD/hour=HH/conn-<id>-<seq>.jsonl.zst
  data/<layer>/<table>/dt=YYYY-MM-DD/hour=HH/part-<HHMMSS>-<seq>.parquet
asset_id is a column, not a partition: at ~75 tokens and 30 s flushes,
per-asset partitions would mean thousands of tiny files per hour.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

from .records import LAYER, TABLES


def _dt_hour(ts_ns: int) -> tuple[str, str]:
    d = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
    return d.strftime("%Y-%m-%d"), d.strftime("%H")


class RawSink:
    """Lines: `<ts_recv_ns>\\t<conn_id>\\t<raw json>\\n`. Raw JSON never contains tabs/newlines."""

    def __init__(self, data_dir: Path, level: int = 19, rotate_minutes: int = 60):
        self.base = data_dir / "raw" / "ws_market"
        self.level = level
        self.rotate_s = rotate_minutes * 60
        self._buf: list[bytes] = []
        self._fh = None
        self._writer = None
        self._opened_at = 0.0
        self._cur_hour = ""
        self._seq = 0
        self.bytes_written = 0  # compressed bytes
        self._tmp_path: Path | None = None
        self._final_path: Path | None = None

    def append(self, ts_recv_ns: int, conn_id: int, raw: str) -> None:
        self._buf.append(f"{ts_recv_ns}\t{conn_id}\t{raw}\n".encode())

    def take(self) -> list[bytes]:
        b, self._buf = self._buf, []
        return b

    # ---- runs in worker thread ---------------------------------------------------

    def write(self, lines: list[bytes]) -> int:
        if not lines:
            return 0
        ts_ns = int(lines[0].split(b"\t", 1)[0])
        dt, hour = _dt_hour(ts_ns)
        if self._writer is None or hour != self._cur_hour or time.time() - self._opened_at > self.rotate_s:
            self._open(dt, hour)
        payload = b"".join(lines)
        self._writer.write(payload)
        return len(payload)

    def _open(self, dt: str, hour: str) -> None:
        self.close()
        self._seq += 1
        d = self.base / f"dt={dt}" / f"hour={hour}"
        d.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%H%M%S")
        self._final_path = d / f"frames-{stamp}-{self._seq:03d}.jsonl.zst"
        self._tmp_path = self._final_path.with_suffix(".zst.tmp")
        self._fh = open(self._tmp_path, "wb")
        # Why a streaming compressor: lines arrive continuously; one zstd frame
        # per file gives the best ratio and `zstd -d` / python can stream it back.
        self._writer = zstd.ZstdCompressor(level=self.level).stream_writer(self._fh)
        self._opened_at = time.time()
        self._cur_hour = hour

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()  # flushes the zstd frame
            self._writer = None
        if self._fh is not None:
            self._fh.close()
            self._fh = None
            if self._tmp_path and self._tmp_path.exists():
                self.bytes_written += self._tmp_path.stat().st_size
                os.replace(self._tmp_path, self._final_path)


class ParquetSink:
    def __init__(self, data_dir: Path, compression: str = "zstd"):
        self.data_dir = data_dir
        self.compression = compression
        self.buffers: dict[str, list[tuple]] = {t: [] for t in TABLES}
        self._seq = 0
        self.bytes_written = 0
        self.rows_written: dict[str, int] = {t: 0 for t in TABLES}

    def pending_rows(self) -> int:
        return sum(len(b) for b in self.buffers.values())

    def take(self) -> dict[str, list[tuple]]:
        """Swap out all buffers (on the event loop, instant)."""
        out = {t: b for t, b in self.buffers.items() if b}
        for t in out:
            self.buffers[t] = []
        return out

    # ---- conversion (on the event loop, in slices) ----------------------------------

    CHUNK = 15_000

    def convert_chunks(self, table: str, rows: list[tuple]):
        """Yield Arrow tables of <= CHUNK rows. Why slices: pa.array() from Python
        objects holds the GIL for the whole call, so converting 100k+ rows in one
        go (even in a thread) freezes the event loop for seconds and starves the
        WS reader -> "slow consumer" disconnects. The recorder awaits between slices."""
        schema = TABLES[table]
        for i in range(0, len(rows), self.CHUNK):
            part = rows[i : i + self.CHUNK]
            cols = list(zip(*part))
            yield pa.Table.from_arrays([pa.array(c, type=f.type) for c, f in zip(cols, schema)], schema=schema)

    # ---- runs in worker thread (I/O only) --------------------------------------------

    def write_tables(self, tables: dict[str, list[pa.Table]], first_ts: dict[str, int]) -> int:
        total = 0
        for table, parts in tables.items():
            if not parts:
                continue
            tbl = pa.concat_tables(parts)
            dt, hour = _dt_hour(first_ts[table])
            d = self.data_dir / LAYER[table] / table / f"dt={dt}" / f"hour={hour}"
            d.mkdir(parents=True, exist_ok=True)
            self._seq += 1
            stamp = datetime.now(timezone.utc).strftime("%H%M%S")
            final = d / f"part-{stamp}-{self._seq:05d}.parquet"
            tmp = final.with_suffix(".parquet.tmp")
            pq.write_table(tbl, tmp, compression=self.compression)
            n = tmp.stat().st_size
            os.replace(tmp, final)
            self.bytes_written += n
            self.rows_written[table] += tbl.num_rows
            total += n
        return total
