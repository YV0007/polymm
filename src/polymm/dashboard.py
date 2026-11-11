"""`polymm dashboard`: gather every number the research dashboard shows into ONE JSON file.

Why one file: the dashboard (a claude.ai artifact) reads attached files, and one upload per
refresh keeps the daily update a single step; each section of the page is a dataset that
picks its own key out of this file.

Sections (each a list of row objects, so every cell can be traced on the page):
    kpis            one row: the headline figures
    collection      per UTC day: rows recorded per table, GB on disk, source (laptop/server)
    quality         per day and family: strict clean share; per day: FAIL/WARN/PASS counts
    fair_scores     held-out fair-value scores per family, candidate and horizon (D-023)
    fair_minute     btc_15m skill by minute of the window
    fair_w          ŵ(τ): share of the model–book gap the mid closes
    fair_calib      btc_15m calibration bins
    markout_curve   average maker markout per family, reference (mid/fair) and horizon (D-024)
    markout_days    per-day markout at τ* and 60 s, with and without the rebate
    model_metrics   expectation model vs baselines (R², correlation)
    decision        drop the worst x % of fills by the model's score: edge of what is kept
    gate            Gate A verdict per family + the reasons
    checks          pass/fail of every automated check we run (incl. recorder health per day)
    recorder        per UTC day from the `health` table: holes, restarts, parse errors, WS cuts,
                    event-loop freezes, A1 mirror kept share
    history         the headline figures at every past export (for trends)
"""
from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

TABLES = {"book_events": "events", "trades": "events", "tob": "events", "spot_quotes": "spot", "spot_trades": "spot"}


def _rows(path: Path) -> int:
    n = 0
    for f in path.rglob("*.parquet"):
        try:
            n += pq.ParquetFile(f).metadata.num_rows
        except Exception:  # a file being written (.tmp is excluded by the glob) or truncated
            pass
    return n


def _gb(path: Path) -> float:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / 1e9


SERVER_CUT = "2026-11-07"   # the server store's first day (09:35:45.919 UTC); earlier rows are the laptop's


def source_of(day: str) -> str:
    """Which recorder a day's rows came from. Why by date: since 2026-11-10 data/ is a symlink to
    data_vps/, the one store of record, holding the laptop's rows before the server's first
    instant and the server's from then on (merge of 2026-11-10)."""
    if day < SERVER_CUT:
        return "laptop"
    if day == SERVER_CUT:
        return "laptop + server (cut 09:35 UTC)"
    return "server"


def collection(data_dirs: dict[str, Path]) -> list[dict]:
    """Rows per day and table, from the store of record (scanned once; `data_dirs` keeps the old
    shape, every root that is the same folder as an earlier one is skipped)."""
    days: dict[str, dict] = {}
    for root in _unique_roots(data_dirs):
        be = root / "events" / "book_events"
        if not be.exists():
            continue
        for d in sorted(p.name[3:] for p in be.glob("dt=*")):
            if d in days:
                continue
            row = {"date": d, "source": source_of(d)}
            for t, layer in TABLES.items():
                row[t] = _rows(root / layer / t / f"dt={d}")
            row["hours"] = len(list((be / f"dt={d}").glob("hour=*")))
            row["gb"] = round(sum(_gb(root / layer / t / f"dt={d}") for t, layer in TABLES.items())
                              + _gb(root / "raw" / "ws_market" / f"dt={d}") + _gb(root / "raw" / "binance" / f"dt={d}"), 3)
            days[d] = row
    return [days[d] for d in sorted(days)]


def _unique_roots(data_dirs: dict[str, Path] | Path) -> list[Path]:
    """Each physical folder once (data/ → data_vps/ is a symlink since 2026-11-10)."""
    roots = [data_dirs] if isinstance(data_dirs, Path) else list(data_dirs.values())
    seen, out = set(), []
    for r in roots:
        k = r.resolve() if r.exists() else r
        if k not in seen:
            seen.add(k)
            out.append(r)
    return out


_HEALTH_COUNTERS = ["parse_errors", "reconnects", "spot_reconnects", "gaps", "rows_delta", "mirror_dropped", "mirror_kept"]


def recorder_health(data_dirs: dict[str, Path]) -> list[dict]:
    """Per UTC day, from the recorder's own `health` table (one row every 10 s): did it
    run, did it parse, how often did the venue cut the feed, did our event loop freeze.

    Why from `health` and not logs: the table travels with the data in the nightly pull,
    so the dashboard can judge a day without logging into the server.
    Why diffs: the counters are cumulative since process start; a restart (uptime goes
    down) restarts them at 0, so on that row the delta is the new value itself.
    Same source rule as `collection`: a day on the server copy beats the laptop copy."""
    out: dict[str, dict] = {}
    for root in _unique_roots(data_dirs):
        base = root / "events" / "health"
        for dd in sorted(base.glob("dt=*")) if base.exists() else []:
            day = dd.name[3:]
            if day in out:
                continue
            files = sorted(dd.rglob("*.parquet"))
            if not files:
                continue
            # Why diagonal_relaxed: older files lack the columns added later (records.py rule).
            h = pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed").sort("ts_ns")
            restart = pl.col("uptime_s").diff().fill_null(0) < 0
            deltas = [pl.when(restart).then(pl.col(c)).otherwise(pl.col(c).diff()).fill_null(0).clip(0).sum().alias(c)
                      for c in _HEALTH_COUNTERS if c in h.columns]
            agg = h.select(
                pl.len().alias("rows"),
                ((pl.col("ts_ns").diff() / 1e9).max()).alias("max_gap_s"),
                restart.sum().alias("restarts"),
                pl.col("loop_lag_max_ms").max().alias("lag_max_ms") if "loop_lag_max_ms" in h.columns else pl.lit(None).alias("lag_max_ms"),
                (pl.col("loop_lag_max_ms") > 1000).sum().alias("lag_over_1s") if "loop_lag_max_ms" in h.columns else pl.lit(None).alias("lag_over_1s"),
                *deltas,
            ).row(0, named=True)
            mk, md = agg.get("mirror_kept") or 0, agg.get("mirror_dropped") or 0
            out[day] = {
                "date": day, "source": source_of(day), "health_rows": agg["rows"],
                "max_gap_s": round(agg["max_gap_s"] or 0, 1), "restarts": agg["restarts"],
                "parse_errors": agg.get("parse_errors"), "ws_cuts": agg.get("reconnects"),
                "spot_cuts": agg.get("spot_reconnects"), "gaps": agg.get("gaps"),
                "loop_lag_max_ms": round(agg["lag_max_ms"], 1) if agg["lag_max_ms"] is not None else None,
                "loop_lag_over_1s": agg["lag_over_1s"],
                "mirror_kept_pct": round(100 * mk / (mk + md), 3) if mk + md else None,
            }
    return [out[d] for d in sorted(out)]


def recorder_checks(rows: list[dict]) -> list[dict]:
    """One pass/fail row per day and question. Thresholds, and why:
    - health gap ≤ 60 s: rows come every 10 s; a planned deploy restart leaves a ~30 s hole
      (measured 11-07/11-08), anything longer means the recorder was down or frozen.
    - parse errors = 0: any error means the venue changed a message format.
    - WS cuts ≤ 24/day: venue-side `1013 slow consumer` cuts on btc_15m (D-022) run at ~15–20/day,
      each a ~1 s hole; more than one an hour means the feed is degrading.
    - event loop never frozen > 1 s: a freeze delays ts_recv on every message behind it.
    - mirror kept < 1 %: A1 assumes outcome 1 is almost always the exact mirror of outcome 0."""
    chk = []
    for r in rows:
        d = r["date"]
        chk.append({"area": "recorder", "check": f"{d}: health rows every 10 s, longest hole ≤ 60 s",
                    "result": f"{r['health_rows']} rows · longest hole {r['max_gap_s']} s · {r['restarts']} restarts",
                    "ok": r["max_gap_s"] <= 60})
        chk.append({"area": "recorder", "check": f"{d}: parse errors = 0", "result": str(r["parse_errors"]), "ok": r["parse_errors"] == 0})
        chk.append({"area": "recorder", "check": f"{d}: venue WS cuts ≤ 24 (spot cuts)",
                    "result": f"{r['ws_cuts']} ({r['spot_cuts']})", "ok": (r["ws_cuts"] or 0) <= 24})
        if r["loop_lag_max_ms"] is not None:
            chk.append({"area": "recorder", "check": f"{d}: event loop never frozen > 1 s",
                        "result": f"max {r['loop_lag_max_ms']:.0f} ms · {r['loop_lag_over_1s']} intervals > 1 s",
                        "ok": r["loop_lag_max_ms"] <= 1000})
        if r["mirror_kept_pct"] is not None:
            chk.append({"area": "recorder", "check": f"{d}: A1 outcome-1 rows kept < 1 %",
                        "result": f"{r['mirror_kept_pct']} %", "ok": r["mirror_kept_pct"] < 1})
    return chk


def quality(day_dirs: list[Path]) -> tuple[list[dict], list[dict]]:
    """Parse each day's quality.md: the FAIL/WARN/PASS line and the strict clean share rows."""
    status, clean = [], []
    for dd in day_dirs:
        q = dd / "quality.md"
        if not q.exists():
            continue
        txt = q.read_text()
        m = re.search(r"\*\*(\d+) FAIL · (\d+) WARN · (\d+) PASS · (\d+) SKIP\*\*", txt)
        if m:
            status.append({"date": dd.name, "fail": int(m[1]), "warn": int(m[2]), "pass": int(m[3]), "skip": int(m[4])})
        for fam, val in re.findall(r"\| coverage \| clean share \(strict\) \| (\w+) \| ([\d.]+) % \|", txt):
            clean.append({"date": dd.name, "family": fam, "strict_clean_pct": float(val)})
    return status, clean


def _records(p: Path, cols: list[str] | None = None, where: pl.Expr | None = None) -> list[dict]:
    if not p.exists():
        return []
    d = pl.read_parquet(p)
    if where is not None:
        d = d.filter(where)
    if cols:
        d = d.select([c for c in cols if c in d.columns])
    d = d.with_columns([pl.col(c).cast(pl.String) for c, t in d.schema.items() if t == pl.Categorical])
    out = []
    for r in d.iter_rows(named=True):
        out.append({k: (None if isinstance(v, float) and (v != v or v in (float("inf"), float("-inf"))) else
                        round(v, 6) if isinstance(v, float) else v) for k, v in r.items()})
    return out


def markout_curve(p: Path) -> list[dict]:
    """table_signature has one row per (family, column, sample); columns are h_mid/h_fair (the
    spread captured at the fill, τ = 0), M<τ>_<ref> and M_res (to resolution). Long form for charts."""
    if not p.exists():
        return []
    s = pl.read_parquet(p).filter(pl.col("sample") == "balanced")
    rows = []
    for r in s.iter_rows(named=True):
        col = r["col"]
        m = re.fullmatch(r"(h|M(\d+))_(mid|fair)", col)
        if not m:
            continue
        tau = 0 if m[1] == "h" else int(m[2])
        rows.append({"family": r["family"], "ref": m[3], "tau": tau, "mean_c": round(r["mean"], 4),
                     "se_c": round(r["se"], 4), "n": r["n"], "clusters": r["n_clusters"]})
    return sorted(rows, key=lambda x: (x["family"], x["ref"], x["tau"]))


def run_tests(repo: Path) -> dict:
    r = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"], cwd=repo, capture_output=True, text=True)
    last = (r.stdout.strip().splitlines() or [""])[-1]
    passed = int(m[1]) if (m := re.search(r"(\d+) passed", last)) else 0
    failed = int(m[1]) if (m := re.search(r"(\d+) failed", last)) else 0
    return {"passed": passed, "failed": failed, "summary": last}


def queue_section(q_root: Path) -> tuple[dict, dict, list[dict]]:
    """Stage 5 (D-026): BTC 15m fill probability, toxicity and value by placement type and queue
    position, latency, our Gate A slice, plus the Q0 checks. Reads reports/queue only."""
    g = json.loads((q_root / "gate_ours.json").read_text()) if (q_root / "gate_ours.json").exists() else {}
    if not g:
        return {}, {}, []
    btc = pl.col("family") == "btc_15m"
    # Why two policies only: "stale" (cancel when left alone or the fair value crosses our price, 128 ms)
    # is what a quoter would run; "passive" (no cancel rule) shows what that rule is worth.
    pol = pl.col("policy").is_in(["stale", "passive"])
    fill = _records(q_root / "table_fill_type.parquet", ["policy", "family", "type", "model", "T", "n", "n_clusters", "mean", "se"], btc & pol)
    val = _records(q_root / "table_value_type.parquet", ["policy", "family", "type", "model", "tau", "n", "n_clusters", "mean", "se"], btc & pol)
    val_nr = _records(q_root / "table_value_type_norebate.parquet", ["policy", "family", "type", "model", "tau", "n", "mean", "se"], btc & pol)
    data = {
        "queue_fill": fill,
        "queue_value": [r | {"rebate": True} for r in val] + [r | {"rebate": False} for r in val_nr],
        "queue_value_x": _records(q_root / "table_value_x.parquet", ["policy", "qtb", "model", "tau", "n", "mean", "se"], pol),
        "queue_tox_x": _records(q_root / "table_tox_x.parquet", ["policy", "model", "qpos", "n", "h", "M1", "M60", "a1", "a1_se", "a60", "a60_se", "D1", "D60"], pol),
        "queue_cancel_latency": _records(q_root / "table_cancel_latency.parquet", ["type", "model", "tau", "lc_ms", "n", "mean", "se"]),
        "queue_latency": _records(q_root / "table_latency.parquet", ["policy", "col", "model", "T", "tau", "lat_ms", "n", "mean", "se"], pl.col("policy") == "stale"),
        "queue_gate": _records(q_root / "table_gate_ours.parquet", ["policy", "rebate", "model", "tau", "date", "n", "n_clusters", "mean", "se"], pol),
    }
    def pick(rows, **kv):
        return next((r for r in rows if all(r.get(k) == v for k, v in kv.items())), {})
    kp = {
        "queue_probes": g.get("probes"),
        "queue_fill60_touch": pick(fill, policy="stale", type="touch", model="matched", T=60).get("mean"),
        "queue_fill60_improve": pick(fill, policy="stale", type="improve", model="matched", T=60).get("mean"),
        "queue_v60_touch_c": pick(val, policy="stale", type="touch", model="matched", tau=60).get("mean"),
        "queue_v60_improve_c": pick(val, policy="stale", type="improve", model="matched", tau=60).get("mean"),
        "queue_v60_touch_passive_c": pick(val, policy="passive", type="touch", model="matched", tau=60).get("mean"),
        "queue_gate_ours": ((g.get("gate_ours") or {}).get("stale") or {}).get("no_rebate"),
    }
    chk = []
    for d, c in sorted(g.get("checks", {}).items()):
        chk += [
            {"area": "queue", "check": f"level stream {d}: size_before = previous size_after", "result": f"{c.get('continuity_break_share', 1) * 100:.3f} % breaks", "ok": c.get("continuity_break_share", 1) < 0.001},
            {"area": "queue", "check": f"traded volume linked to a book decrease {d}", "result": f"{c.get('linked_volume_share', 0) * 100:.1f} %", "ok": c.get("linked_volume_share", 0) >= 0.95},
            {"area": "queue", "check": f"level size = top-of-book size {d}", "result": f"bid {c.get('touch_size_match', {}).get('bid', 0) * 100:.2f} % · ask {c.get('touch_size_match', {}).get('ask', 0) * 100:.2f} %",
             "ok": min(c.get("touch_size_match", {}).get("bid", 0), c.get("touch_size_match", {}).get("ask", 0)) >= 0.99},
        ]
    return data, kp, chk


def rewards_section(rw_root: Path) -> tuple[dict, dict, list[dict]]:
    """Stage 6 (D-029): the sector grid of liquidity-reward yields over every paying market, the top
    markets, and our recorded Fed pools (reward vs fill P&L per share). Reads reports/rewards only."""
    j = rw_root / "rewards.json"
    if not j.exists():
        return {}, {}, []
    R = json.loads(j.read_text())
    sector = _records(rw_root / "table_screen_sector.parquet",
                      ["sector", "markets", "pool_usd_day", "yield1k_med", "yield1k_p75", "yield1k_med_cons", "yield10k_med",
                       "markets_cons_over_0_2pct", "capacity_med_usd", "capacity_sum_usd", "jump_med_c", "days_to_end_med", "two_sided_only_share"])
    top = _records(rw_root / "table_screen_top.parquet",
                   ["sector", "question", "rate_per_day", "mid", "spread_c", "comp_central", "yield1k_central", "yield1k_high",
                    "yield10k_central", "capacity_usd", "days_to_end", "snaps"])[:25]
    # the headline day (latest recorded) only: 11-06's book was ~5x thinner, so pooled shares mislead
    hd = R.get("headline_day")
    pool = _records(rw_root / "table_pool_per_share.parquet",
                    ["question", "date", "policy", "model", "reward_c_per_share_side", "fill_c_per_share_side", "fill_c_per_share_side_se"],
                    pl.col("date") == hd) if hd else []
    data = {"rewards_sector": sector, "rewards_top": top, "rewards_pool": pool}
    best = max(sector, key=lambda r: r.get("yield1k_med") or 0) if sector else {}
    fed = next((r for r in pool if r.get("policy") == "passive" and r.get("model") == "matched" and "increase" in (r.get("question") or "")), {})
    sc = R.get("screen") or {}
    kp = {"rewards_live_markets": (R.get("live") or {}).get("markets"), "rewards_pool_usd_day": (R.get("live") or {}).get("pool_usd_day"),
          "rewards_recorded_pool_usd_day": sum(x.get("pool_usd_day") or 0 for x in R.get("recorded_with_pool") or []),
          "rewards_best_sector": best.get("sector"), "rewards_best_sector_yield1k": best.get("yield1k_med"),
          "rewards_fed_reward_c": fed.get("reward_c_per_share_side"), "rewards_fed_fill_c": fed.get("fill_c_per_share_side"),
          "rewards_fed_fill_c_se": fed.get("fill_c_per_share_side_se"), "rewards_screen_snaps": sc.get("snapshots")}
    chk = [{"area": "rewards", "check": "reward configuration snapshot is from the last 2 days", "result": str(R.get("config_day")),
            "ok": bool(R.get("config_day")) and (datetime.now(timezone.utc) - datetime.fromisoformat(R["config_day"]).replace(tzinfo=timezone.utc)).days <= 2},
           {"area": "rewards", "check": "screen used ≥ 3 book snapshots per market", "result": f"{sc.get('snapshots')} snapshots, {sc.get('markets_sampled')} markets",
            "ok": (sc.get("snapshots") or 0) >= 3}]
    return data, kp, chk


def sim_section(sim_root: Path) -> tuple[dict, dict, list[dict]]:
    """Stage 7 (D-032): the strategy ladder's walk-forward P&L per window with the four terms per
    filled share, the fill-model band, the latency curve, Gate B and the engine checks.
    Reads reports/sim only."""
    j = sim_root / "sim.json"
    if not j.exists():
        return {}, {}, []
    R = json.loads(j.read_text())
    rungs = _records(sim_root / "table_rungs.parquet")
    for r in rungs:
        sh = r.get("shares") or 0
        for c in ("c1_spread", "c2_adverse", "c3_inventory", "c4_rebate"):
            r[c + "_c"] = round(100 * r[c] / sh, 4) if sh and r.get(c) is not None else None
    keep = ["rung", "fold", "windows", "mean", "se", "pnl", "fills", "shares", "sharpe_w", "worst_window", "q_peak",
            "c1_spread_c", "c2_adverse_c", "c3_inventory_c", "c4_rebate_c"]
    rungs = [{k: r.get(k) for k in keep} for r in rungs]
    band = _records(sim_root / "table_band.parquet", ["rung", "fold", "model", "windows", "mean", "se"])
    lat = _records(sim_root / "table_latency.parquet", ["rung", "fold", "lat_ms", "mean", "se", "fills"])
    fed = _records(sim_root / "table_fed.parquet") if (sim_root / "table_fed.parquet").exists() else []
    gate = [{"rung": int(k), **v} for k, v in (R.get("gate_b_by_rung") or {}).items()]
    # Why fills > 0: a rung that sat out has mean exactly 0, which would "win" against losing rungs
    test = [r for r in rungs if r["fold"] == "test" and r.get("mean") is not None and (r.get("fills") or 0) > 0]
    best = max(test, key=lambda r: r["mean"]) if test else {}
    lb = next((r for r in rungs if r["fold"] == "lockbox" and r["rung"] == best.get("rung")), {})
    nogap = _records(sim_root / "table_rungs_nogap.parquet", ["rung", "fold", "windows", "mean", "se", "pnl"]) \
        if (sim_root / "table_rungs_nogap.parquet").exists() else []
    data = {"sim_rungs": rungs, "sim_rungs_nogap": nogap, "sim_band": band, "sim_latency": lat, "sim_gate": gate, "sim_fed": fed}
    kp = {"sim_gate_b": R.get("gate_b"), "sim_best_rung": best.get("rung"), "sim_best_mean": best.get("mean"), "sim_best_se": best.get("se"),
          "sim_best_lockbox_mean": lb.get("mean"), "sim_lockbox": R.get("lockbox"), "sim_test_days": ", ".join(R.get("test_days") or []),
          "sim_settings": R.get("settings"), "sim_lat_ms": R.get("lat_base_ms")}
    chk = []
    ch = R.get("checks") or {}
    for d, c in sorted(ch.items()):
        inp = (c or {}).get("inputs") or {}
        if inp:
            chk.append({"area": "simulator", "check": f"{d}: ①+②+③ = hold-to-settlement P&L", "result": f"max error {inp.get('identity_err_max', 0):.1e}",
                        "ok": (inp.get("identity_err_max") or 0) < 1e-9})
            chk.append({"area": "simulator", "check": f"{d}: fair value at whole seconds = stage 3/4 walk-forward series", "result": str(inp.get("fair_grid_max_abs_err")),
                        "ok": inp.get("fair_grid_max_abs_err") == 0.0})
            te = inp.get("fair_tick_at_own_spot_max_abs_err")
            chk.append({"area": "simulator", "check": f"{d}: tick formula at the second's own BTC price = that second's fair value", "result": str(te),
                        "ok": te is not None and te < 1e-6})
            chk.append({"area": "simulator", "check": f"{d}: no fills or order-log rows truncated", "result": str(inp.get("overflow_total")),
                        "ok": inp.get("overflow_total") == 0})
        if "do_nothing_fills" in (c or {}):
            chk.append({"area": "simulator", "check": f"{d}: do-nothing strategy has no fills", "result": str(c["do_nothing_fills"]), "ok": c["do_nothing_fills"] == 0})
        rep = (c or {}).get("stage5_reproduction") or {}
        if rep:
            ok = all(v.get("fill_qty_exact") == 1.0 and v.get("reject_agree") == 1.0 for k, v in rep.items() if isinstance(v, dict))
            chk.append({"area": "simulator", "check": f"{d}: stage-5 probes reproduced exactly", "result": f"{rep.get('probes')} probes, 3 cancel models", "ok": ok})
    viol = R.get("delay_violations") or []
    chk.append({"area": "simulator", "check": "more latency never pays (beyond 2 SE)", "result": f"{len(viol)} violations", "ok": not viol})
    nz = R.get("noise_check") or {}
    chk.append({"area": "simulator", "check": "a noise 'fair value' does not beat the true one", "result": f"{sum(v['noisy_not_better_2se'] for v in nz.values())} of {len(nz)}",
                "ok": all(v["noisy_not_better_2se"] for v in nz.values()) if nz else False})
    return data, kp, chk


def diag_section(root: Path) -> tuple[dict, dict, list[dict]]:
    """Strategy diagnosis (D-034): what each lever is worth at most (oracles), the loss by how the
    fill happened, the markout curve of our fills against the average maker. Reads reports/sim_diag."""
    if not (root / "table_oracles.parquet").exists():
        return {}, {}, []
    orc = _records(root / "table_oracles.parquet", ["rung", "oracle", "mean", "gain_per_window", "gain_se"])
    mech = _records(root / "table_by_mech.parquet", ["rung", "mech", "fills", "share_of_shares", "c1_c", "c2_c", "adv_0_1s_c", "net_c", "usd", "share_of_pnl"])
    sig = _records(root / "table_signature.parquet", ["rung", "tau_ms", "markout_c"])
    ms = root / "table_maker_signature.parquet"
    if ms.exists():
        sig += [{"rung": "maker", **r} for r in _records(ms, ["tau_ms", "markout_c"], pl.col("tau_ms") <= 60000)]
    race = _records(root / "table_race_by_size.parquet") if (root / "table_race_by_size.parquet").exists() else []
    pred = _records(root / "table_predictability.parquet", ["test_day", "r2_oos", "corr"]) if (root / "table_predictability.parquet").exists() else []
    s1 = {r["tau_ms"]: r["markout_c"] for r in sig if r["rung"] == 1}
    get = lambda r, o: next((x for x in orc if x["rung"] == r and x["oracle"] == o), {})
    big = next((x for x in race if x.get("jump_size") == ">=4c"), {})
    kp = {"diag_loss_within_1s_r1": (s1[0] - s1[1000]) / (s1[0] - s1[60000]) if s1.get(0) is not None and s1.get(60000) not in (None, s1.get(0)) else None,
          "diag_base_r1": get(1, "base").get("mean"), "diag_lead1s_gain_r1": get(1, "fair_lead_1s").get("gain_per_window"),
          "diag_cancel0_gain_r1": get(1, "cancel0").get("gain_per_window"), "diag_lat0_gain_r1": get(1, "lat0").get("gain_per_window"),
          "diag_preempted_big": big.get("share_preempted_500ms"), "diag_preempted_big_control": big.get("share_opposite_500ms_control"),
          "diag_pred_r2": max((p["r2_oos"] for p in pred), default=None)}
    return {"diag_oracles": orc, "diag_mech": mech, "diag_signature": sig}, kp, []


def build(repo: Path, out: Path, fair_root: Path, mk_root: Path, data_dirs: dict[str, Path], tests: bool = True,
          q_root: Path = Path("reports/queue"), rw_root: Path = Path("reports/rewards"), sim_root: Path = Path("reports/sim"), diag_root: Path = Path("reports/sim_diag")) -> dict:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=repo, capture_output=True, text=True).stdout.strip()
    fair_params = json.loads((fair_root / "fair_params.json").read_text()) if (fair_root / "fair_params.json").exists() else {}
    gate = json.loads((mk_root / "gate.json").read_text()) if (mk_root / "gate.json").exists() else {}
    coll = collection(data_dirs)
    day_dirs = sorted(p for p in fair_root.glob("20*-*-*") if p.is_dir())
    q_status, q_clean = quality(day_dirs)
    fair_scores = _records(fair_root / "table_headline.parquet",
                           ["family", "cand", "tau", "n", "n_clusters", "skill", "skill_se", "b", "b_se", "hit", "brier", "brier_mid"])
    t = run_tests(repo) if tests else {"passed": None, "failed": None, "summary": "not run"}

    def pick(rows, **kv):
        for r in rows:
            if all(r.get(k) == v for k, v in kv.items()):
                return r
        return {}

    f30 = pick(fair_scores, family="btc_15m", cand="x_fair", tau=30)
    s30 = pick(fair_scores, family="btc_15m", cand="x_stoikov", tau=30)
    curve = markout_curve(mk_root / "table_signature.parquet")
    h0 = pick(curve, family="btc_15m", ref="fair", tau=0)
    m1 = pick(curve, family="btc_15m", ref="fair", tau=1)
    m60 = pick(curve, family="btc_15m", ref="fair", tau=60)
    dec = _records(mk_root / "table_decision.parquet")
    keep_half = pick(dec, family="btc_15m", scorer="ridge", drop=0.5)
    all_fills = pick(dec, family="btc_15m", scorer="const", drop=0.0)
    checks = gate.get("checks", {})
    kpis = {
        "generated_at": now, "commit": commit,
        "data_through": max((r["date"] for r in coll), default=None),
        "days_recorded": len(coll), "gb_recorded": round(sum(r["gb"] for r in coll), 2),
        "book_rows_m": round(sum(r["book_events"] for r in coll) / 1e6, 1),
        "fills_analyzed": gate.get("fills_used"),
        "fair_test_days": ", ".join(fair_params.get("dates", [])[1:]),
        "fair_skill30_15m": f30.get("skill"), "fair_skill30_15m_se": f30.get("skill_se"),
        "stoikov_skill30_15m": s30.get("skill"),
        "fair_brier_15m": f30.get("brier"), "mid_brier_15m": f30.get("brier_mid"),
        "h_fair_15m_c": h0.get("mean_c"), "m1_fair_15m_c": m1.get("mean_c"), "m60_fair_15m_c": m60.get("mean_c"),
        "edge_all_15m_c": all_fills.get("edge_c"), "edge_keep_half_15m_c": keep_half.get("edge_c"),
        "gate_verdict": gate.get("verdict"), "gate_15m": gate.get("verdict_family", {}).get("btc_15m"),
        "tau_star_15m_s": gate.get("tau_star", {}).get("btc_15m"),
        "tests_passed": t["passed"], "tests_failed": t["failed"],
    }
    # checks: everything automated that can pass or fail, one row each
    chk = []
    for r in q_status:
        chk.append({"area": "data quality", "check": f"quality report {r['date']}", "result": f"{r['fail']} FAIL · {r['warn']} WARN · {r['pass']} PASS",
                    "ok": r["fail"] == 0})
    fs = fair_root / "summary.md"
    if fs.exists():
        txt = fs.read_text()
        if (m := re.search(r"reference: `(\{.*?\})`", txt)):
            ref = ast.literal_eval(m[1])  # a dict literal in our own summary file
            chk += [
                {"area": "fair value", "check": "15m strike chain: finalPrice(k) = priceToBeat(k+1)", "result": f"{ref['chain_pairs'] - ref['chain_mismatch']} of {ref['chain_pairs']}", "ok": ref["chain_mismatch"] == 0},
                {"area": "fair value", "check": "Up iff final ≥ strike", "result": f"{ref['rule_checked'] - ref['rule_mismatch']} of {ref['rule_checked']}", "ok": ref["rule_mismatch"] == 0},
                {"area": "fair value", "check": "Gamma outcome = our recorded resolution", "result": f"{ref['ws_resolutions_compared'] - ref['ws_resolution_mismatch']} of {ref['ws_resolutions_compared']}", "ok": ref["ws_resolution_mismatch"] == 0},
                {"area": "fair value", "check": "ladder strikes parsed", "result": f"{ref['ladder_markets'] - ref['ladder_strike_missing']} of {ref['ladder_markets']}", "ok": ref["ladder_strike_missing"] == 0},
            ]
        chk.append({"area": "fair value", "check": "no input received after t (lookahead)", "result": "asserted on every row", "ok": "inputs " in txt})
    for d, c in sorted(checks.items()):
        chk.append({"area": "adverse selection", "check": f"fills {d}: trade side consistent with the book", "result": f"{c.get('sign_ok', 0) * 100:.1f} %", "ok": c.get("sign_ok", 0) >= 0.95})
    q_data, q_kpis, q_chk = queue_section(q_root)
    kpis.update(q_kpis)
    chk += q_chk
    r_data, r_kpis, r_chk = rewards_section(rw_root)
    kpis.update(r_kpis)
    chk += r_chk
    s_data, s_kpis, s_chk = sim_section(sim_root)
    kpis.update(s_kpis)
    chk += s_chk
    g_data, g_kpis, _ = diag_section(diag_root)
    kpis.update(g_kpis)
    rec = recorder_health(data_dirs)
    chk += recorder_checks(rec)
    chk.append({"area": "code", "check": "test suite", "result": t["summary"], "ok": bool(t["passed"]) and not t["failed"]})
    # history: one snapshot per data date (a rerun on the same data replaces it)
    hist_path = out.parent / "history.json"
    hist = json.loads(hist_path.read_text()) if hist_path.exists() else []
    snap = {k: kpis[k] for k in ("data_through", "generated_at", "days_recorded", "fills_analyzed", "fair_skill30_15m",
                                 "fair_brier_15m", "m1_fair_15m_c", "edge_keep_half_15m_c", "tests_passed")}
    hist = [h for h in hist if h.get("data_through") != snap["data_through"]] + [snap]
    hist.sort(key=lambda h: h.get("data_through") or "")
    data = {
        "kpis": [kpis],
        "collection": coll,
        "quality": q_status,
        "clean": q_clean,
        "fair_scores": fair_scores,
        "fair_minute": _records(fair_root / "table_by_minute_15m.parquet", ["cand", "min_b", "tau", "n", "skill", "skill_se", "hit"]),
        "fair_w": _records(fair_root / "table_w_hat.parquet", ["family", "min_b", "absd_b", "hte_b", "tau", "w_hat", "w_se", "n"]),
        "fair_calib": _records(fair_root / "table_calibration_15m.parquet", ["col", "bin", "pred", "obs", "n", "brier"]),
        "markout_curve": curve,
        "markout_days": _records(mk_root / "table_gate_unconditional.parquet",
                                 ["family", "scope", "tau", "n", "h_fair", "M", "M_se", "M_reb", "M_reb_se", "M60", "M60_se", "M60_reb", "M60_reb_se"]),
        "model_metrics": _records(mk_root / "table_model_metrics.parquet"),
        "decision": [r for r in dec if r["family"] == "btc_15m"],
        "gate": [{"family": f, "verdict": v, "tau_star_s": gate.get("tau_star", {}).get(f),
                  "reasons": " | ".join(x for x in gate.get("verdict_why", []) if f in x)} for f, v in gate.get("verdict_family", {}).items()],
        "checks": chk,
        "recorder": rec,
        "history": hist,
        **q_data,
        **r_data,
        **s_data,
        **g_data,
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, allow_nan=False, default=str))
    tmp.replace(out)
    hist_path.write_text(json.dumps(hist, indent=1))
    return data
