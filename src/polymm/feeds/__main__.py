"""`polymm-feeds`: the external-feeds pipeline (D-042).

  polymm-feeds record  [-c config.yaml] [--feeds a,b]        24/7 on the VPS, raw only
  polymm-feeds parse   [--date YYYY-MM-DD|yesterday] [--force] [--feeds a,b]
  polymm-feeds upload  [--root data] [--layers ...] [--prune-days N] [--dry]
  polymm-feeds fetch   --prefix feeds/tables/binance_perp_bbo/dt=2026-11-20 [--prefix ...] [--root data]
  polymm-feeds calendar [--from D --to D]
Why its own entry point: `polymm.cli` is the Polymarket recorder's; this pipeline deploys,
restarts and fails on its own.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import date
from pathlib import Path

from ..config import load_config


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="polymm-feeds")
    ap.add_argument("-c", "--config", default="config.yaml")
    ap.add_argument("--s3-env", default="~/.config/polymm/s3.env")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("record")
    r.add_argument("--feeds", default=None, help="comma list; default all")
    p = sub.add_parser("parse")
    p.add_argument("--date", default="yesterday")
    p.add_argument("--feeds", default=None)
    p.add_argument("--force", action="store_true")
    u = sub.add_parser("upload")
    u.add_argument("--root", default=None, help="data root; default the config's data dir")
    u.add_argument("--layers", default=None, help="comma list; default raw,events,spot,reference,backfill,feeds")
    u.add_argument("--prune-days", type=int, default=None, help="delete uploaded feeds partitions older than N days")
    u.add_argument("--dry", action="store_true")
    f = sub.add_parser("fetch")
    f.add_argument("--prefix", action="append", required=True)
    f.add_argument("--root", default=None)
    f.add_argument("--dry", action="store_true")
    c = sub.add_parser("calendar")
    c.add_argument("--from", dest="start", default=None)
    c.add_argument("--to", dest="end", default=None)
    a = ap.parse_args(argv)

    cfg = load_config(a.config)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per REST poll otherwise
    data = cfg.data_dir

    if a.cmd == "record":
        from .recorder import FeedRecorder

        feeds = a.feeds.split(",") if a.feeds else None
        logging.getLogger().addHandler(logging.FileHandler(cfg.log_dir / "feeds.log"))
        asyncio.run(FeedRecorder(data, feeds).run())
    elif a.cmd == "parse":
        from .parse import parse_day, yesterday

        day = yesterday() if a.date == "yesterday" else a.date
        chk = parse_day(data / "feeds", day, a.feeds.split(",") if a.feeds else None, a.force)
        print((data / "feeds" / "checks" / f"dt={day}" / "checks.md").read_text())
        print(f"{chk['seconds']} s")
    elif a.cmd == "upload":
        from .upload import DEFAULT_LAYERS, run_upload

        root = Path(a.root) if a.root else data
        layers = tuple(a.layers.split(",")) if a.layers else DEFAULT_LAYERS
        print(run_upload(root, a.s3_env, layers, a.prune_days, dry=a.dry))
    elif a.cmd == "fetch":
        from .upload import run_fetch

        print(run_fetch(Path(a.root) if a.root else data, a.s3_env, a.prefix, a.dry))
    elif a.cmd == "calendar":
        from .calendar import load_calendar

        with __import__("polars").Config(tbl_rows=200, fmt_str_lengths=60):
            print(load_calendar(date.fromisoformat(a.start) if a.start else None, date.fromisoformat(a.end) if a.end else None))


if __name__ == "__main__":
    main()
