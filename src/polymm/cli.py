"""Command line: `polymm universe | record | audit | backfill | backfill-spot`."""
from __future__ import annotations

import argparse
import asyncio
import logging
import logging.handlers
import sys
from pathlib import Path

from .config import load_config


def _setup_logging(log_dir: Path, name: str) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)
    # Why rotating file: launchd/systemd capture stdout, but a local log file
    # survives a service manager restart and is what `audit` points you to.
    fh = logging.handlers.RotatingFileHandler(log_dir / f"{name}.log", maxBytes=20_000_000, backupCount=5)
    fh.setFormatter(fmt)
    root.addHandler(fh)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)


def cmd_universe(args) -> int:
    from .rest import PolyRest
    from .universe import UniverseWatcher

    cfg = load_config(args.config)

    async def _run():
        rest = PolyRest(cfg.endpoints)
        uw = UniverseWatcher(cfg, rest)
        print("desired slugs now:")
        for fam, slug in uw.desired_slugs():
            print(f"  [{fam.name}] {slug}")
        added, _, _ = await uw.refresh()
        print(f"\nresolved {len(added)} markets / {len(uw.desired_tokens())} tokens:")
        for mi in added:
            print(
                f"  [{mi.family}] {mi.question[:70]!r}\n"
                f"      ends {mi.end_date}  tick {mi.tick_size}  fee {mi.fee_rate}  "
                f"rewards {mi.rewards_min_size}/{mi.rewards_max_spread}  $/day {mi.rate_per_day}"
            )
            for aid, outcome in mi.tokens:
                print(f"      {outcome:>5}  {aid}")
        await rest.aclose()

    asyncio.run(_run())
    return 0


def cmd_record(args) -> int:
    from .recorder import Recorder

    cfg = load_config(args.config)
    _setup_logging(cfg.log_dir, "recorder")
    logging.getLogger(__name__).info("starting recorder; data_dir=%s", cfg.data_dir)
    rec = Recorder(cfg)
    asyncio.run(rec.run())
    return 0


def cmd_audit(args) -> int:
    from .audit import run_audit

    cfg = load_config(args.config)
    return run_audit(cfg, args.date)


def cmd_backfill(args) -> int:
    from .backfill import run_backfill

    cfg = load_config(args.config)
    _setup_logging(cfg.log_dir, "backfill")
    return asyncio.run(run_backfill(cfg, args.what))


def cmd_backfill_spot(args) -> int:
    from .spot import backfill_spot

    cfg = load_config(args.config)
    _setup_logging(cfg.log_dir, "backfill")
    rc = 0
    for date in args.dates:
        try:
            if backfill_spot(cfg, args.symbol, date, args.kind) is None:
                rc = 2  # not published yet: let cron retry tomorrow
        except Exception as e:
            logging.getLogger(__name__).exception("backfill-spot %s %s failed: %r", args.symbol, date, e)
            rc = 1
    return rc


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="polymm")
    p.add_argument("-c", "--config", default="config.yaml")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("universe", help="show which markets/tokens the rules resolve to right now")
    sub.add_parser("record", help="run the recorder (Ctrl-C / SIGTERM to stop cleanly)")
    a = sub.add_parser("audit", help="summarise one day of recorded data")
    a.add_argument("--date", default=None, help="YYYY-MM-DD (UTC); default today")
    b = sub.add_parser("backfill", help="fetch historical trades / prices / resolutions for recorded markets")
    b.add_argument("what", nargs="?", default="all", choices=["all", "trades", "prices", "resolutions"])
    bs = sub.add_parser("backfill-spot", help="fetch Binance daily spot history (data.binance.vision) into parquet")
    bs.add_argument("dates", nargs="+", help="YYYY-MM-DD (UTC) ... ; a day is published the next UTC morning")
    bs.add_argument("--symbol", default="BTCUSDT")
    bs.add_argument("--kind", default="trades", choices=["trades", "aggTrades"])
    args = p.parse_args(argv)
    return {
        "universe": cmd_universe, "record": cmd_record, "audit": cmd_audit,
        "backfill": cmd_backfill, "backfill-spot": cmd_backfill_spot,
    }[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
