"""Command line: `polymm universe | record | audit | backfill | backfill-spot | replay | quality | shape | vol | fair | markouts`."""
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


def cmd_replay(args) -> int:
    from datetime import datetime, timedelta, timezone

    from .replay import NS, ReplayConfig, Replayer, parse_ts, validation_report

    cfg = load_config(args.config)
    data_dir = Path(args.data) if args.data else cfg.data_dir
    # Why --date as the default window: one UTC day is the unit the audit and the
    # Parquet layout use; --t0/--t1 narrow or override it.
    day = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    t0 = parse_ts(args.t0) if args.t0 else int(day.timestamp() * NS)
    t1 = parse_ts(args.t1) if args.t1 else int((day + timedelta(days=1)).timestamp() * NS)
    out = None if args.validate_only else Path(args.out)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    rc = ReplayConfig(data_dir=data_dir, out_dir=out, t0=t0, t1=t1, assets=set(args.asset) if args.asset else None,
                      chunk_s=args.chunk_s)
    rp = Replayer(rc)
    stats = rp.run()
    print(validation_report(rp.rest_checks_all, stats, ws_mismatches=rp.ws_mismatches, ws_lags=rp.ws_lags))
    if out is not None:
        print(f"derived tables written under {out / 'derived'}")
    return 0


def cmd_quality(args) -> int:
    from datetime import datetime, timedelta, timezone

    from .quality import QualityConfig, run_quality, summary_line
    from .replay import NS, parse_ts

    cfg = load_config(args.config)
    data_dir = Path(args.data) if args.data else cfg.data_dir
    day = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    t0 = parse_ts(args.t0) if args.t0 else int(day.timestamp() * NS)
    t1 = parse_ts(args.t1) if args.t1 else int((day + timedelta(days=1)).timestamp() * NS)
    # Why reports/<date> by default: one folder per day holds the replay, the
    # report and the strict windows every later analysis starts from.
    out = Path(args.out) if args.out else Path("reports") / args.date
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    qc = QualityConfig(date=args.date, data_dir=data_dir, out_dir=out,
                       replay_out=Path(args.replay_out) if args.replay_out else None, t0=t0, t1=t1)
    res = run_quality(qc)
    print(f"quality {args.date}: {summary_line(res)}")
    for c in res.checks:
        if c.status in ("FAIL", "WARN"):
            print(f"  {c.status} {c.section}/{c.name} [{c.family}] = {c.value_str()} ({c.threshold}) {c.note}")
    print(f"report: {out / 'quality.md'}")
    return 0


def cmd_shape(args) -> int:
    from datetime import datetime, timedelta, timezone

    from .replay import NS, parse_ts
    from .shape import ShapeConfig, run_shape

    cfg = load_config(args.config)
    data_dir = Path(args.data) if args.data else cfg.data_dir
    day = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    t0 = parse_ts(args.t0) if args.t0 else int(day.timestamp() * NS)
    t1 = parse_ts(args.t1) if args.t1 else int((day + timedelta(days=1)).timestamp() * NS)
    # Why reports/<date>/shape by default: next to the quality report and the
    # strict windows it reads, one folder per day.
    qdir = Path(args.quality_dir) if args.quality_dir else Path("reports") / args.date
    out = Path(args.out) if args.out else qdir / "shape"
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    sc = ShapeConfig(date=args.date, data_dir=data_dir, out_dir=out, quality_dir=qdir,
                     replay_out=Path(args.replay_out) if args.replay_out else None, t0=t0, t1=t1)
    T = run_shape(sc)
    print(f"shape {args.date}: {len(T)} tables; report: {out / 'shape.md'}")
    return 0


def cmd_vol(args) -> int:
    from .vol import VolConfig, run_vol

    cfg = load_config(args.config)
    data_dir = Path(args.data) if args.data else cfg.data_dir
    # Why a separate --spot-data: the laptop dataset has the Polymarket books of the clean
    # block but no Binance tables; those live in the VPS copy.
    out = Path(args.out) if args.out else Path("reports") / args.date / "vol"
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    vc = VolConfig(date=args.date, data_dir=data_dir, out_dir=out, spot_dir=Path(args.spot_data) if args.spot_data else None,
                   replay_out=Path(args.replay_out) if args.replay_out else None,
                   strict_path=Path(args.strict) if args.strict else None, plots=not args.no_plots)
    btc, poly = run_vol(vc)
    print(f"vol {args.date}: chosen BTC estimator {btc.chosen.get('estimator')} (QLIKE {btc.chosen.get('qlike', float('nan')):.3f})")
    if poly is not None and "sigma_x_family" in poly.tables:
        import polars as pl
        for row in poly.tables["sigma_x_family"].filter(pl.col("k_s") == 10).iter_rows(named=True):
            print(f"  sigma_x(10 s) {row['family']:<18} {row['sigma_x']:.4f}  (n={row['n']:,})")
        s = poly.tables.get("implied_15m_summary")
        if s is not None and s.height:
            r = s.row(0, named=True)
            print(f"  btc_15m implied/realized median {r['median_ratio_impl_over_realized_60s']:.2f} over {r["n_windows_fit"]} windows")
    print(f"outputs: {out}/summary.md, sigma_btc.parquet, tables/, *.png")
    return 0


def cmd_fair(args) -> int:
    from .fair.run import FairConfig, run_fair

    # Why per-day folders under --root: each day needs its replay, strict windows and σ
    # (`polymm quality` + `polymm vol` with --out <root>/<date>); fair reads them, and the
    # first date is training only (walk-forward).
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    fc = FairConfig(dates=sorted(args.dates), root=Path(args.root), spot_root=Path(args.spot_data),
                    data_dirs=[Path(d) for d in args.data], plot_dir=Path(args.plots_out), fit_tau=args.fit_tau,
                    refresh_ref=not args.no_refresh, plots=not args.no_plots)
    R = run_fair(fc)
    print(f"fair {', '.join(fc.dates)}: {R['oos_rows']:,} held-out rows in {R['seconds']} s; report {fc.root / 'summary.md'}")
    return 0


def cmd_markouts(args) -> int:
    from .markouts.run import MarkoutConfig, run_markouts

    # Why on top of the fair day folders: markouts must use the same rebuilt books and clean
    # windows as the fair value they are measured against; the first date is training only
    # (it fixes τ* and trains the first model fold).
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    mc = MarkoutConfig(dates=sorted(args.dates), fair_root=Path(args.fair_root), out=Path(args.out),
                       data_dirs=[Path(d) for d in args.data], plot_dir=Path(args.plots_out), plots=not args.no_plots,
                       reuse=args.reuse, spot_root=Path(args.spot_data), fair_oos=not args.in_sample_fair)
    R = run_markouts(mc)
    print(f"markouts {', '.join(mc.dates)}: {R['fills_used']:,} fills, τ* {R['tau_star']}, Gate A {R['verdict']} "
          f"{R['verdict_family']} in {R['seconds']} s; report {mc.out / 'summary.md'}")
    return 0


def cmd_queue(args) -> int:
    from .queue.run import QueueConfig, run_queue

    # Why on top of the markouts output: probe fills are scored with the same walk-forward fair
    # series (reports/markouts/<date>/fair_oos.parquet) and markout engine as the stage-4 fills.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    qc = QueueConfig(dates=sorted(args.dates), fair_root=Path(args.fair_root), markouts_root=Path(args.markouts_root),
                     out=Path(args.out), data_dirs=[Path(d) for d in args.data], plot_dir=Path(args.plots_out),
                     plots=not args.no_plots, reuse=args.reuse)
    R = run_queue(qc)
    print(f"queue {', '.join(qc.dates)}: {R['probes']:,} probes, x* {R['x_star']}, our Gate A slice {R['gate_ours']} "
          f"in {R['seconds']} s; report {qc.out / 'summary.md'}")
    return 0


def cmd_rewards(args) -> int:
    from .rewards.run import RewardsConfig, run_rewards

    # Why --sample: only the current reward configuration and books are published, so the screen
    # needs live REST snapshots (each ~20 s over ~11k markets); the daily refresh takes a few.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    rc = RewardsConfig(dates=sorted(args.dates), fair_root=Path(args.fair_root), queue_root=Path(args.queue_root), out=Path(args.out),
                       data_root=Path(args.data), plot_dir=Path(args.plots_out), plots=not args.no_plots, sample=args.sample, every_s=args.every)
    R = run_rewards(rc)
    print(f"rewards: {R['live']['markets']:,} live pools ({R['live']['pool_usd_day']:,.0f} USD/day), screen {R['screen']} in {R['seconds']} s; "
          f"report {rc.out / 'summary.md'}")
    return 0


def cmd_sim(args) -> int:
    from .sim.run import SimConfig, run_sim

    # Why on top of stages 3–6: the engine replays stage 5's level streams and queue model, the fair
    # value is stage 3's walk-forward series moved by every Binance tick, rung 4 reads stage-5 probes
    # of earlier days, rung 5 scores rewards with stage 6's rules.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    sc = SimConfig(dates=sorted(args.dates), fair_root=Path(args.fair_root), queue_root=Path(args.queue_root), out=Path(args.out),
                   data_dirs=[Path(d) for d in args.data], spot_root=Path(args.spot_root), plot_dir=Path(args.plots_out),
                   plots=not args.no_plots, reuse=args.reuse, fed=not args.no_fed)
    R = run_sim(sc)
    print(f"sim {', '.join(sc.dates)}: {R['settings']} settings, test {R['test_days']}, lockbox {R['lockbox']}, Gate B {R['gate_b']}; "
          f"report {sc.out / 'summary.md'}")
    return 0


def cmd_diagnose(args) -> int:
    from .sim.diagnose import run

    # Why on top of `polymm sim`: it diagnoses the settings the simulator's walk-forward selected,
    # on the same days and inputs (D-034).
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    R = run(sim_root=Path(args.sim_root), out=Path(args.out), plot_dir=None if args.no_plots else Path(args.plots_out))
    print(f"diagnose {', '.join(R['res']['days'])}: rungs {R['res']['rungs']}, {len(R['res']['oracles'])} runs each; tables in {args.out}")
    return 0


def cmd_dashboard(args) -> int:
    from .dashboard import build

    # Why a separate command: the research dashboard reads ONE JSON file; this gathers the
    # latest fair-value and markout outputs, row counts, checks and tests into it.
    out = Path(args.out)
    d = build(Path("."), out, Path(args.fair_root), Path(args.markouts_root),
              {"store": Path(args.server_data)}, tests=not args.no_tests)
    k = d["kpis"][0]
    print(f"dashboard data -> {out} ({out.stat().st_size / 1e3:.0f} kB): data through {k['data_through']}, "
          f"tests {k['tests_passed']} passed / {k['tests_failed']} failed, Gate A {k['gate_verdict']}")
    return 0


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
    r = sub.add_parser("replay", help="rebuild books from stored events, write derived tables, validate against REST snapshots")
    r.add_argument("--date", required=True, help="YYYY-MM-DD (UTC day to replay)")
    r.add_argument("--out", default=None, help="output root; derived tables go under <out>/derived/ (required unless --validate-only)")
    r.add_argument("--data", default=None, help="dataset root (default: paths.data_dir from config)")
    r.add_argument("--asset", action="append", default=None, help="restrict to this asset_id (repeatable)")
    r.add_argument("--t0", default=None, help="start, ISO UTC or ns (default: start of --date)")
    r.add_argument("--t1", default=None, help="end (exclusive), ISO UTC or ns (default: end of --date)")
    r.add_argument("--chunk-s", type=int, default=600, help="seconds of events held in memory at once")
    r.add_argument("--validate-only", action="store_true", help="print the REST match report; write nothing")
    q = sub.add_parser("quality", help="replay (or reuse) a day, run the data-quality checks, write reports/<date>/quality.md")
    q.add_argument("--date", required=True, help="YYYY-MM-DD (UTC day)")
    q.add_argument("--data", default=None, help="dataset root (default: paths.data_dir from config)")
    q.add_argument("--replay-out", default=None, help="replay output root; reused if <dir>/derived exists (default: <out>/replay)")
    q.add_argument("--out", default=None, help="report folder (default: reports/<date>)")
    q.add_argument("--t0", default=None, help="start, ISO UTC or ns (default: start of --date)")
    q.add_argument("--t1", default=None, help="end (exclusive), ISO UTC or ns (default: end of --date)")
    sh = sub.add_parser("shape", help="stage 2 stylized facts per family (spread, depth, queue, flow, trades, jumps) inside strict clean windows")
    sh.add_argument("--date", required=True, help="YYYY-MM-DD (UTC day)")
    sh.add_argument("--data", default=None, help="dataset root (default: paths.data_dir from config)")
    sh.add_argument("--replay-out", default=None, help="replay output root with <dir>/derived (default: <quality-dir>/replay)")
    sh.add_argument("--quality-dir", default=None, help="folder holding clean_windows_strict.parquet (default: reports/<date>)")
    sh.add_argument("--out", default=None, help="report folder (default: <quality-dir>/shape)")
    sh.add_argument("--t0", default=None, help="start, ISO UTC or ns (default: start of --date)")
    sh.add_argument("--t1", default=None, help="end (exclusive), ISO UTC or ns (default: end of --date)")
    v = sub.add_parser("vol", help="volatility: BTC realized/EWMA σ, forecast scores, Polymarket σ_x per family, implied vs realized")
    v.add_argument("--date", required=True, help="YYYY-MM-DD (UTC day)")
    v.add_argument("--data", default=None, help="Polymarket dataset root (default: paths.data_dir from config)")
    v.add_argument("--spot-data", default=None, help="root holding spot/ and backfill/ (default: --data)")
    v.add_argument("--replay-out", default=None, help="replay output root holding derived/ (default: reports/<date>/replay)")
    v.add_argument("--strict", default=None, help="clean_windows_strict.parquet (default: reports/<date>/clean_windows_strict.parquet)")
    v.add_argument("--out", default=None, help="output folder (default: reports/<date>/vol)")
    v.add_argument("--no-plots", action="store_true", help="skip PNGs (matplotlib not needed)")
    fv = sub.add_parser("fair", help="fair value v1: inputs, candidates, walk-forward blend, scores, report (step 6)")
    fv.add_argument("dates", nargs="+", help="YYYY-MM-DD ... ; the first is training only")
    fv.add_argument("--root", default="reports/fair", help="per-day folders with replay/, clean_windows_strict.parquet, vol/")
    fv.add_argument("--spot-data", default="data_vps", help="root holding spot/ and backfill/")
    fv.add_argument("--data", nargs="+", default=["data", "data_vps"], help="dataset roots with reference/markets")
    fv.add_argument("--plots-out", default="research/fair_value", help="PNG folder")
    fv.add_argument("--fit-tau", type=int, default=30, help="horizon (s) the blend weight is fitted on")
    fv.add_argument("--no-refresh", action="store_true", help="reuse the cached Gamma reference")
    fv.add_argument("--no-plots", action="store_true")
    mk = sub.add_parser("markouts", help="adverse selection: fill table, markouts, signature, slices, model, Gate A (stage 4)")
    mk.add_argument("dates", nargs="+", help="YYYY-MM-DD ... ; the first is training only (fixes τ*)")
    mk.add_argument("--fair-root", default="reports/fair", help="fair-value day folders (replay/, clean windows, reference/)")
    mk.add_argument("--out", default="reports/markouts")
    mk.add_argument("--data", nargs="+", default=["data_vps", "data"], help="event stores, in preference order")
    mk.add_argument("--spot-data", default="data_vps", help="root holding spot/ (for the walk-forward fair series)")
    mk.add_argument("--plots-out", default="research/adverse_selection")
    mk.add_argument("--reuse", action="store_true", help="reuse per-day markouts.parquet and fair_oos.parquet")
    mk.add_argument("--in-sample-fair", action="store_true", help="use reports/fair/<date>/fair.parquet (final fit; leaks, for comparison only)")
    mk.add_argument("--no-plots", action="store_true")
    qu = sub.add_parser("queue", help="queue and fill model: virtual orders on the replayed book, fill probability, toxicity by queue position, value (stage 5)")
    qu.add_argument("dates", nargs="+", help="YYYY-MM-DD ... ; the first is training only")
    qu.add_argument("--fair-root", default="reports/fair", help="fair-value day folders (replay/, clean windows, reference/)")
    qu.add_argument("--markouts-root", default="reports/markouts", help="stage-4 output (walk-forward fair series, signature)")
    qu.add_argument("--out", default="reports/queue")
    qu.add_argument("--data", nargs="+", default=["data_vps", "data"], help="event stores, in preference order")
    qu.add_argument("--plots-out", default="research/queue_fill")
    qu.add_argument("--reuse", action="store_true", help="reuse per-day probes.parquet (analysis-only rerun)")
    qu.add_argument("--no-plots", action="store_true")
    rw = sub.add_parser("rewards", help="liquidity rewards and fees: pools, our share, cost of resting, screen over every paying market (stage 6)")
    rw.add_argument("dates", nargs="+", help="recorded days for the pool markets we record (YYYY-MM-DD ...)")
    rw.add_argument("--fair-root", default="reports/fair")
    rw.add_argument("--queue-root", default="reports/queue", help="stage-5 probes (cost of resting)")
    rw.add_argument("--out", default="reports/rewards")
    rw.add_argument("--data", default="data", help="where reward configs and book samples are stored (reference/)")
    rw.add_argument("--plots-out", default="research/rewards")
    rw.add_argument("--sample", type=int, default=0, help="take N REST snapshots of every paying market's book first")
    rw.add_argument("--every", type=int, default=600, help="seconds between snapshots")
    rw.add_argument("--no-plots", action="store_true")
    sm = sub.add_parser("sim", help="simulator: our quotes in the replayed book, strategy ladder, walk-forward, fill band, latency, Gate B (stage 7)")
    sm.add_argument("dates", nargs="+", help="YYYY-MM-DD ... ; the first is training only, the last is the lockbox")
    sm.add_argument("--fair-root", default="reports/fair")
    sm.add_argument("--queue-root", default="reports/queue", help="stage-5 probes (rung 4's value table, the reproduction check)")
    sm.add_argument("--out", default="reports/sim")
    sm.add_argument("--data", nargs="+", default=["data_vps", "data"], help="event stores, in preference order")
    sm.add_argument("--spot-root", default="data_vps", help="Binance quotes (fair value between seconds)")
    sm.add_argument("--plots-out", default="research/simulator")
    sm.add_argument("--reuse", action="store_true", help="reuse pass-1 grid results of days already run")
    sm.add_argument("--no-fed", action="store_true", help="skip the Fed rewards rung")
    sm.add_argument("--no-plots", action="store_true")
    dg = sub.add_parser("diagnose", help="strategy diagnosis: fill anatomy, markout curve, oracle levers, the race, selection (stage 7)")
    dg.add_argument("--sim-root", default="reports/sim")
    dg.add_argument("--out", default="reports/sim_diag")
    dg.add_argument("--plots-out", default="research/strategy_diagnosis")
    dg.add_argument("--no-plots", action="store_true")
    db = sub.add_parser("dashboard", help="gather all dashboard numbers into one JSON file")
    db.add_argument("--out", default="reports/dashboard/dashboard_data.json")
    db.add_argument("--fair-root", default="reports/fair")
    db.add_argument("--markouts-root", default="reports/markouts")
    db.add_argument("--server-data", default="data_vps")
    db.add_argument("--no-tests", action="store_true", help="skip running the test suite")
    args = p.parse_args(argv)
    if args.cmd == "replay" and not args.validate_only and not args.out:
        p.error("replay: --out is required unless --validate-only")
    return {
        "universe": cmd_universe, "record": cmd_record, "audit": cmd_audit,
        "backfill": cmd_backfill, "backfill-spot": cmd_backfill_spot, "replay": cmd_replay, "quality": cmd_quality,
        "shape": cmd_shape,
        "vol": cmd_vol,
        "fair": cmd_fair,
        "markouts": cmd_markouts,
        "queue": cmd_queue,
        "rewards": cmd_rewards,
        "sim": cmd_sim,
        "diagnose": cmd_diagnose,
        "dashboard": cmd_dashboard,
    }[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
