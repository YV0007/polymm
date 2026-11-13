"""Daily parse (D-042): raw frames of one UTC day → Parquet tables, hour by hour, plus checks.

   data/feeds/raw/<feed>/dt=D/hour=H/*.jsonl.zst  ──►  data/feeds/tables/<table>/dt=D/hour=H/part-<feed>.parquet
                                                  ──►  data/feeds/checks/dt=D/checks.json, checks.md

Why hour by hour: the biggest hour (Binance perp best bid/ask, ~2.3 M rows) is ~200 MB in
memory; a day at once would be ~5 GB and grow with every feed added. Why once a day and not
in the recorder: the recorder stays a stamping loop; a parser change is a re-parse of raw,
not lost data; and the checks need the whole hour to count gaps.
"""
from __future__ import annotations

import io
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import zstandard as zstd

from .parsers import PARSER_VERSION, SCHEMAS, new_rows, parse_line, to_table
from .spec import FEEDS

log = logging.getLogger(__name__)

# Which tables carry a venue stamp, for the staleness check (ts_recv − ts_venue, ms)
STALENESS_TABLES = [t for t, s in SCHEMAS.items() if "ts_venue" in s.names]


def hours_of(root: Path, feed: str, day: str) -> list[str]:
    d = root / "raw" / feed / f"dt={day}"
    return sorted(p.name.split("=")[1] for p in d.glob("hour=*")) if d.exists() else []


def iter_lines(path: Path):
    with open(path, "rb") as fh, zstd.ZstdDecompressor().stream_reader(fh) as r:
        for line in io.TextIOWrapper(r, encoding="utf-8", newline="\n", errors="replace"):
            yield line.encode()


def parse_hour(root: Path, feed: str, day: str, hour: str) -> dict:
    """Parse one feed-hour; write its tables; return its check numbers."""
    src = root / "raw" / feed / f"dt={day}" / f"hour={hour}"
    files = sorted(p for p in src.glob("*.jsonl.zst"))
    rows = new_rows()
    errors: list = []
    n_lines = 0
    for f in files:
        for line in iter_lines(f):
            n_lines += 1
            parse_line(feed, line, rows, errors)
    out_tables = {}
    for t, r in rows.items():
        if not r:
            continue
        tbl = to_table(t, r)
        d = root / "tables" / t / f"dt={day}" / f"hour={hour}"
        d.mkdir(parents=True, exist_ok=True)
        tmp = d / f"part-{feed}.parquet.tmp"
        pq.write_table(tbl, tmp, compression="zstd")
        os.replace(tmp, d / f"part-{feed}.parquet")
        out_tables[t] = tbl.num_rows
    chk = {"lines": n_lines, "files": len(files), "rows": out_tables, "parse_errors": len(errors),
           "error_samples": [e for e in errors if e][:5], "reconnects": len(rows["feeds_reconnects"])}
    chk["staleness_ms"] = _staleness(rows)
    chk["gaps"] = _gaps(feed, rows)
    return chk


def _staleness(rows) -> dict:
    out = {}
    for t in STALENESS_TABLES:
        r = rows.get(t)
        if not r:
            continue
        i = SCHEMAS[t].get_field_index("ts_venue")
        d = pl.Series([(x[0] - x[i]) / 1e6 for x in r if x[i] is not None])
        if d.len():
            out[t] = {"n": d.len(), "p50": round(float(d.quantile(0.5)), 1), "p99": round(float(d.quantile(0.99)), 1),
                      "min": round(float(d.min()), 1)}
    return out


def _gaps(feed: str, rows) -> dict:
    """Sequence checks a venue makes possible. A gap = a message we did not receive."""
    g = {}
    if feed in ("binance_perp_book", "binance_spot_depth") and rows["binance_depth"]:
        # one row per level; the sequence is per message: collapse on (conn, first_id, last_id, prev)
        seen, gaps, n = None, 0, 0
        for r in rows["binance_depth"]:
            key = (r[1], r[5], r[6], r[7])
            if key == seen:
                continue
            n += 1
            if seen is not None and seen[0] == key[0]:
                ok = (key[3] == seen[2]) if feed == "binance_perp_book" else (key[1] == seen[2] + 1)
                gaps += 0 if ok else 1
            seen = key
        g["binance_depth"] = {"msgs": n, "gaps": gaps}
    if feed == "bybit" and rows["bybit_book"]:
        seen, gaps, n = None, 0, 0
        for r in rows["bybit_book"]:
            key = (r[1], r[5])
            if key == seen:
                continue
            n += 1
            if seen is not None and seen[0] == key[0] and r[4] == "delta" and key[1] != seen[1] + 1:
                gaps += 1
            seen = key
        g["bybit_book"] = {"msgs": n, "gaps": gaps}
    if feed == "coinbase" and rows["coinbase_heartbeat"]:
        hb = rows["coinbase_heartbeat"]
        # consecutive heartbeats one second apart: sequence jumps bigger than the messages we
        # got in between would need the ticker counts; here: count heartbeats and resets
        g["coinbase_heartbeat"] = {"msgs": len(hb), "sequence_resets": sum(1 for a, b in zip(hb, hb[1:]) if b[1] == a[1] and b[4] < a[4])}
    if feed == "okx" and rows["okx_bbo"]:
        s = [r[4] for r in rows["okx_bbo"] if r[4] is not None]
        g["okx_bbo"] = {"msgs": len(s), "non_increasing": sum(1 for a, b in zip(s, s[1:]) if b <= a)}
    return g


def parse_day(root: Path, day: str, feeds: list[str] | None = None, force: bool = False) -> dict:
    """All feeds of a day. Skips feed-hours whose output already carries PARSER_VERSION."""
    t0 = time.perf_counter()
    feeds = feeds or list(FEEDS)
    checks: dict = {"day": day, "parser_version": PARSER_VERSION, "feeds": {}}
    for feed in feeds:
        fc = {}
        for hour in hours_of(root, feed, day):
            if not force and _done(root, feed, day, hour):
                fc[hour] = {"skipped": "already parsed with this version"}
                continue
            try:
                fc[hour] = parse_hour(root, feed, day, hour)
            except Exception as e:  # a broken hour must not stop the other 23
                log.exception("parse %s %s %s failed", feed, day, hour)
                fc[hour] = {"failed": repr(e)}
            log.info("parsed %s %s %s: %s", feed, day, hour, {k: v for k, v in fc[hour].items() if k in ("lines", "parse_errors", "reconnects")})
        checks["feeds"][feed] = fc
    checks["seconds"] = round(time.perf_counter() - t0, 1)
    out = root / "checks" / f"dt={day}"
    out.mkdir(parents=True, exist_ok=True)
    (out / "checks.json").write_text(json.dumps(checks, indent=1))
    (out / "checks.md").write_text(checks_md(checks))
    return checks


def _done(root: Path, feed: str, day: str, hour: str) -> bool:
    """Parsed already if any table file of this feed-hour exists with the current version
    (hours with zero rows leave no file and are re-parsed: cheap)."""
    for t in SCHEMAS:
        f = root / "tables" / t / f"dt={day}" / f"hour={hour}" / f"part-{feed}.parquet"
        if f.exists():
            md = pq.read_schema(f).metadata or {}
            return md.get(b"parser_version") == str(PARSER_VERSION).encode()
    return False


def checks_md(c: dict) -> str:
    L = [f"# Feeds parse — {c['day']} (parser v{c['parser_version']}, {c.get('seconds', 0)} s)", "",
         "| feed | hours | lines | rows | parse errors | reconnects | gaps | staleness ms, p50/p99 per table |", "|---|---|---|---|---|---|---|---|"]
    for feed, hrs in c["feeds"].items():
        done = [h for h in hrs.values() if "lines" in h]
        lines = sum(h["lines"] for h in done)
        rows = sum(sum(h["rows"].values()) for h in done)
        errs = sum(h["parse_errors"] for h in done)
        rec = sum(h["reconnects"] for h in done)
        gaps = defaultdict(int)
        for h in done:
            for t, g in h["gaps"].items():
                for k, v in g.items():
                    if k != "msgs":
                        gaps[f"{t}.{k}"] += v
        # staleness pooled over the hours: n-weighted p50 and the worst p99, per table
        acc: dict[str, list] = defaultdict(list)
        for h in done:
            for t, s in h["staleness_ms"].items():
                acc[t].append(s)
        st = "; ".join(f"{t.replace('binance_', '').replace('deribit_', '')} {sum(s['p50'] * s['n'] for s in v) / sum(s['n'] for s in v):.0f}/{max(s['p99'] for s in v):.0f}"
                       for t, v in sorted(acc.items(), key=lambda kv: -sum(s['n'] for s in kv[1]))[:4])
        L.append(f"| {feed} | {len(done)}/{len(hrs)} | {lines} | {rows} | {errs} | {rec} | {dict(gaps) or '–'} | {st} |")
    return "\n".join(L) + "\n"


def yesterday() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
