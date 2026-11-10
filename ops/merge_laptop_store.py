"""One-off (2026-11-10, D-030): merge the laptop recording into the server store, no duplicates.

    python -I ops/merge_laptop_store.py [--apply]       (dry run without --apply)

The server copy (data_vps/) is the store of record from its first recorded instant
T = 2026-11-07 09:35:45.919 UTC (its first `markets` row). The laptop recording (data/) is
kept only for t < T, the part the server never saw. After the merge:
  data_vps/  holds the full chain: laptop rows before T, server rows from T on, plus one
             `reconnect` marker at T (replay resets every book there and starts again from the
             server's first snapshot instead of splicing two recorders' books together)
  data/      becomes a symlink to data_vps/, so every default path keeps working
  ~/.Trash/polymm-laptop-duplicates-<date>/  holds what the laptop recorded from T on
             (duplicates of the server's copy, recoverable until the Trash is emptied)

Why the server wins after T: over the overlap (11-07 09:35 → 11-10 11:08) the server holds
2–6× more book changes and trades; the laptop-only rows are ≤ 0.2 % per day and sit in the
server's one-second connection cuts (comparison in D-030).

Hours are moved as whole folders when they lie entirely before T; the one hour that
straddles T (2026-11-07 hour=09) is rewritten: laptop rows with time < T are written to a new
file next to the server's files. Nothing is deleted outright.
"""
from __future__ import annotations

import io
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import zstandard as zstd

ROOT = Path(__file__).resolve().parents[1]
LAP, SRV = ROOT / "data", ROOT / "data_vps"
T = 1794044145919710978  # ns, server's first `markets` row (fetched_at_ns)
T_DT, T_HOUR = "2026-11-07", "09"
TRASH = Path.home() / ".Trash" / f"polymm-laptop-duplicates-{datetime.now(timezone.utc):%Y-%m-%d}"
TIME_COL = {"health": "ts_ns", "markets": "fetched_at_ns"}  # every other table: ts_recv_ns
TABLE_DIRS = [p for p in sorted((LAP / "events").glob("*"))] + [LAP / "reference" / "markets", LAP / "raw" / "ws_market"]


def plan():
    move, split, dup = [], [], []
    for tdir in TABLE_DIRS:
        for dtdir in sorted(tdir.glob("dt=*")):
            dt = dtdir.name[3:]
            for hdir in sorted(dtdir.glob("hour=*")):
                h = hdir.name[5:]
                key = (dt, h)
                if key < (T_DT, T_HOUR):
                    move.append(hdir)
                elif key == (T_DT, T_HOUR):
                    split.append(hdir)
                else:
                    dup.append(hdir)
    return move, split, dup


def rel(p: Path) -> Path:
    return p.relative_to(LAP)


def split_parquet(hdir: Path, apply: bool) -> tuple[int, int]:
    table = hdir.parent.parent.name
    col = TIME_COL.get(table, "ts_recv_ns")
    files = sorted(hdir.glob("*.parquet"))
    if not files:
        return 0, 0
    t = pa.concat_tables([pq.read_table(f) for f in files], promote_options="default")
    keep = t.filter(pc.less(t[col], pa.scalar(T, pa.int64())))
    if apply and keep.num_rows:
        out = SRV / rel(hdir) / "part-laptop-before-server.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        assert not out.exists(), out
        pq.write_table(keep, out.with_suffix(".parquet.tmp"), compression="zstd")
        out.with_suffix(".parquet.tmp").replace(out)
    return t.num_rows, keep.num_rows


def split_raw(hdir: Path, apply: bool) -> tuple[int, int]:
    n_in = n_keep = 0
    kept: list[bytes] = []
    for f in sorted(hdir.glob("*.zst")):
        with open(f, "rb") as fh:
            rd = io.BufferedReader(zstd.ZstdDecompressor().stream_reader(fh, read_across_frames=True))
            for line in rd:
                n_in += 1
                ts = int(line.split(b"\t", 1)[0]) if line[:1].isdigit() else None
                if ts is not None and ts < T:
                    kept.append(line)
                    n_keep += 1
    if apply and kept:
        out = SRV / rel(hdir) / "frames-laptop-before-server.jsonl.zst"
        out.parent.mkdir(parents=True, exist_ok=True)
        assert not out.exists(), out
        tmp = out.with_suffix(".zst.tmp")
        with open(tmp, "wb") as fh, zstd.ZstdCompressor(level=12).stream_writer(fh) as w:
            w.write(b"".join(kept))
        tmp.replace(out)
    return n_in, n_keep


def junction_marker(apply: bool) -> None:
    """One global `reconnect` row at T (book_events schema, as the recorder writes it)."""
    from polymm.records import BOOK_EVENTS
    row = {f.name: [None] for f in BOOK_EVENTS}
    row.update(ts_recv_ns=[T], ts_exch_ms=[0], asset_id=[""], condition_id=[""], event_type=["reconnect"], hash=[""], conn_id=[0])
    tbl = pa.table({f.name: pa.array(row[f.name], type=f.type) for f in BOOK_EVENTS}, schema=BOOK_EVENTS)
    out = SRV / "events" / "book_events" / f"dt={T_DT}" / f"hour={T_HOUR}" / "part-junction-laptop-to-server.parquet"
    if apply:
        assert not out.exists(), out
        pq.write_table(tbl, out)
    print(f"junction marker -> {out.relative_to(ROOT)}")


def main(apply: bool) -> int:
    assert LAP.is_dir() and not LAP.is_symlink(), "data/ is already merged (symlink) or missing"
    move, split, dup = plan()
    print(f"T = {datetime.fromtimestamp(T / 1e9, tz=timezone.utc):%Y-%m-%d %H:%M:%S.%f} UTC")
    print(f"move into data_vps: {len(move)} hour folders; split at T: {len(split)}; duplicates to Trash: {len(dup)}")
    for h in move:
        dst = SRV / rel(h)
        if dst.exists():
            print(f"CONFLICT: {rel(h)} exists in data_vps; stopping"); return 1
    for h in split:
        if h.parent.parent.name == "ws_market":
            n, k = split_raw(h, apply)
        else:
            n, k = split_parquet(h, apply)
        print(f"split {rel(h)}: keep {k:,} of {n:,} rows (before T)")
    if sys.path[0] != str(ROOT / "src"):
        sys.path.insert(0, str(ROOT / "src"))
    junction_marker(apply)
    if not apply:
        print("dry run: nothing changed")
        return 0
    for h in move:
        dst = SRV / rel(h)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(h), str(dst))
    TRASH.mkdir(parents=True, exist_ok=False)
    shutil.move(str(LAP), str(TRASH / "data"))  # what is left: split-hour originals + duplicates
    LAP.symlink_to("data_vps", target_is_directory=True)
    print(f"moved {len(move)} hour folders; laptop leftovers -> {TRASH}; data -> data_vps")
    return 0


if __name__ == "__main__":
    sys.exit(main("--apply" in sys.argv))
