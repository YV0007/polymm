"""`polymm sim`: stage 7 end to end (D-032).

    pass 1  every grid setting × every day, matched cancels, 75 ms        → <date>/grid_windows.parquet
    select  walk-forward: test day k uses the best setting on days < k; last day = lockbox
    pass 2  per test/lockbox day, the selected setting of each rung under the four cancel models
            and six latencies, the checks (stage-5 reproduction, random signal, do-nothing),
            and the Fed rewards rung                                         → <date>/band_windows.parquet
    report  tables, Gate B, sim.json / sim_params.json / summary.md, plots

Outputs in reports/sim/ (never in git); write-up in research/simulator.md.
"""
from __future__ import annotations

import gc
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import polars as pl

from ..markouts.fills import fee_table, market_index
from . import attrib as A
from . import day as Dy
from . import engine as E
from . import experiments as X
from . import rewards_sim as RS
from .inputs import NS, check_inputs, fair_seconds, load_sim_day, rebate_k

log = logging.getLogger(__name__)


@dataclass
class SimConfig:
    dates: list[str]
    fair_root: Path = Path("reports/fair")
    queue_root: Path = Path("reports/queue")
    out: Path = Path("reports/sim")
    data_dirs: list[Path] = field(default_factory=lambda: [Path("data_vps"), Path("data")])
    spot_root: Path = Path("data_vps")
    plot_dir: Path = Path("research/simulator")
    plots: bool = True
    reuse: bool = False      # reuse pass-1 results of days whose grid file exists (same fair mode and engine)
    fed: bool = True
    fair_mode: str = "event"  # "event" (book touch + Binance ticks, D-038) or "sec" (book held per second)
    rungs: tuple[int, ...] | None = None  # restrict the grid to these rungs (None = all)


def _load(cfg: SimConfig, d: str, idx: pl.DataFrame, fs_paths: dict) -> tuple[dict, Dy.Prepared]:
    S = load_sim_day(d, cfg.fair_root, cfg.data_dirs, cfg.spot_root, fs_paths[d], idx, fair_mode=cfg.fair_mode)
    return S, Dy.prepare(S, d)


def _tags(cfg: SimConfig) -> dict:
    return {"fair_mode": cfg.fair_mode, "engine": E.ENGINE_VERSION}


def _same_run(path: Path, cfg: SimConfig) -> bool:
    """A stored day may be reused only if it was produced by the same fair stream and engine."""
    if not path.exists():
        return False
    c = json.loads(path.read_text())
    return all(c.get(k) == v for k, v in _tags(cfg).items())


def _one(P: Dy.Prepared, S: dict, c: dict, rk: float, *, model: str, lat: float, seed: int, vt, vn, tag: dict,
         fair_noise_c: float = 0.0, prm: np.ndarray | None = None) -> tuple[pl.DataFrame, float]:
    R = Dy.run_day(P, c["rung"], X.prm_of(c) if prm is None else prm, model=model, lat_ms=lat, seed=seed, vtab=vt, vtab_n=vn,
                   fair_noise_c=fair_noise_c)
    a = A.attribute(R["fills"], S["fair"], R["segments"], rk)
    w = A.per_window(a, R["segments"], X.SIZE)
    return w.with_columns(*[pl.lit(v).alias(k) for k, v in tag.items()]), A.identity_error(a)


def pass1(cfg: SimConfig, dates: list[str], idx, fs_paths, rk, configs) -> pl.DataFrame:
    Ws = []
    for i, d in enumerate(dates):
        p = cfg.out / d / "grid_windows.parquet"
        if cfg.reuse and p.exists() and _same_run(cfg.out / d / "checks.json", cfg):
            Ws.append(pl.read_parquet(p))
            continue
        t0 = time.perf_counter()
        S, P = _load(cfg, d, idx, fs_paths)
        chk = check_inputs(S, Path("reports/markouts") / d / "fair_oos.parquet") | {"queue_checks": S["checks"], "data_root": str(S["root"])}
        vt, vn = X.value_table(dates[:i] or [d], cfg.queue_root)
        W, ie, ovf = [], 0.0, 0
        for c in configs:
            w, e = _one(P, S, c, rk["btc_15m"], model=X.MODEL_BASE, lat=X.LAT_BASE, seed=i, vt=vt, vn=vn,
                        tag={"date": d, "cfg": c["cfg"], "rung": c["rung"], "model": X.MODEL_BASE, "lat_ms": X.LAT_BASE})
            W.append(w)
            ie = max(ie, e)
            ovf += int(w["overflow"].sum())
        W = pl.concat(W, how="diagonal_relaxed")
        p.parent.mkdir(parents=True, exist_ok=True)
        W.write_parquet(p)
        chk |= {"identity_err_max": ie, "overflow_total": ovf, "configs": len(configs), "seconds": round(time.perf_counter() - t0, 1),
                "vtab_cells": int((vn >= 200).sum()), "vtab_train": dates[:i] or [d]} | _tags(cfg)
        (cfg.out / d / "checks.json").write_text(json.dumps(chk, indent=1, default=str))
        log.info("pass 1 %s: %d settings in %.0f s", d, len(configs), time.perf_counter() - t0)
        Ws.append(W)
        del S, P
        gc.collect()
    return pl.concat(Ws, how="diagonal_relaxed")


def stage5_reproduction(P: Dy.Prepared, d: str, queue_root: Path) -> dict:
    """S1 check on real data: stage-5 probes (every 7th grid probe) replayed as scripted orders;
    filled quantity and rejection must match exactly, per cancel model."""
    f = queue_root / d / "probes.parquet"
    if not f.exists():
        return {}
    pr = pl.read_parquet(f).filter((pl.col("family") == "btc_15m") & (pl.col("design") == "grid") & (pl.col("lat_ms") == 128)
                                   & (pl.col("pid") % 7 == 0))
    script = {}
    for (y,), g in pr.sort("t_arr").partition_by("yes", as_dict=True).items():
        script[y] = {"t": g["t_arr"].to_numpy().astype(np.int64), "s": g["s"].to_numpy().astype(np.int64), "pk": g["pk"].to_numpy().astype(np.int64),
                     "sz": np.full(g.height, 5.0), "tc": g["t_stop"].to_numpy().astype(np.int64), "pid": g["pid"].to_numpy()}
    out = {"probes": pr.height}
    for m in ("pess", "matched", "opt"):
        R = Dy.run_day(P, -1, E.params(), model=m, lat_ms=0, script=script, maxf=400000)
        O = pl.concat([g.with_columns(pl.Series("pid", script[y]["pid"][g["oid"].to_numpy()]))
                       for (y,), g in R["orders"].partition_by("yes", as_dict=True).items()])
        F = pl.concat([g.with_columns(pl.Series("pid", script[y]["pid"][g["oid"].to_numpy()]))
                       for (y,), g in R["fills"].partition_by("yes", as_dict=True).items()]).group_by("pid").agg(pl.col("qty").sum().alias("q"))
        j = (pr.select("pid", "stop", pl.col(f"filled5_{m}").alias("q5")).join(O.select("pid", "end"), on="pid", how="left")
             .join(F, on="pid", how="left").with_columns(pl.col("q").fill_null(0.0)))
        out[m] = {"fill_qty_exact": round(float(((j["q5"] - j["q"]).abs() < 1e-6).mean()), 5),
                  "reject_agree": round(float(((j["stop"] == "rejected") == (j["end"] == 3)).mean()), 5)}
    return out


def pass2(cfg: SimConfig, dates: list[str], idx, fs_paths, rk, configs, sel: pl.DataFrame) -> tuple[pl.DataFrame, dict, pl.DataFrame]:
    cf = {c["cfg"]: c for c in configs}
    Bs, checks, fed = [], {}, []
    for i, d in enumerate(dates):
        if i == 0:
            continue
        chosen = sel.filter(pl.col("date") == d)
        bp, fp, cp = cfg.out / d / "band_windows.parquet", cfg.out / d / "fed_windows.parquet", cfg.out / d / "checks_pass2.json"
        # Why reuse is safe here: day k's selection uses only days < k, which never change once
        # recorded, so a stored band for the same selected settings is the same result
        if cfg.reuse and bp.exists() and _same_run(cp, cfg):
            old = pl.read_parquet(bp)
            want = set(zip(chosen["rung"].to_list(), chosen["cfg"].to_list(), chosen["fold"].to_list()))
            have = set(old.filter(pl.col("exp") == "band").select("rung", "cfg", "fold").unique().iter_rows())
            if want == have:
                Bs.append(old)
                checks[d] = json.loads(cp.read_text())
                if cfg.fed and fp.exists():
                    fed.append(pl.read_parquet(fp))
                continue
        t0 = time.perf_counter()
        S, P = _load(cfg, d, idx, fs_paths)
        vt, vn = X.value_table(dates[:i], cfg.queue_root)
        B = []
        for r in chosen.iter_rows(named=True):
            c = cf[r["cfg"]]
            tag = {"date": d, "cfg": c["cfg"], "rung": c["rung"], "fold": r["fold"]}
            for m in X.MODELS:
                w, _ = _one(P, S, c, rk["btc_15m"], model=m, lat=X.LAT_BASE, seed=i, vt=vt, vn=vn, tag=tag | {"model": m, "lat_ms": X.LAT_BASE, "exp": "band"})
                B.append(w)
            for lat in X.LATS:
                w, _ = _one(P, S, c, rk["btc_15m"], model=X.MODEL_BASE, lat=lat, seed=i, vt=vt, vn=vn,
                            tag=tag | {"model": X.MODEL_BASE, "lat_ms": lat, "exp": "latency"})
                B.append(w)
            if c["rung"] in (1, 3):
                for noise in (0.5, 2.0):
                    w, _ = _one(P, S, c, rk["btc_15m"], model=X.MODEL_BASE, lat=X.LAT_BASE, seed=i, vt=vt, vn=vn, fair_noise_c=noise,
                                tag=tag | {"model": X.MODEL_BASE, "lat_ms": X.LAT_BASE, "exp": f"noise_{noise}"})
                    B.append(w)
        # do-nothing: inventory limit 0 → no order can ever be wanted
        c1 = cf[(chosen.filter(pl.col("rung") == 1) if (chosen["rung"] == 1).any() else chosen)["cfg"][0]]
        w, _ = _one(P, S, c1, rk["btc_15m"], model=X.MODEL_BASE, lat=X.LAT_BASE, seed=i, vt=vt, vn=vn,
                    prm=E.params(size=X.SIZE, qmax=0.0), tag={"date": d, "cfg": "do_nothing", "rung": 1, "fold": "check",
                                                              "model": X.MODEL_BASE, "lat_ms": X.LAT_BASE, "exp": "do_nothing"})
        B.append(w)
        B = pl.concat(B, how="diagonal_relaxed")
        B.write_parquet(cfg.out / d / "band_windows.parquet")
        checks[d] = {"do_nothing_fills": int(w["fills"].sum()), "do_nothing_pnl": float(w["pnl"].sum())} | _tags(cfg)
        if i == 1:
            checks[d]["stage5_reproduction"] = stage5_reproduction(P, d, cfg.queue_root)
        if cfg.fed:
            fw = fed_rung(cfg, P, S, d, rk)
            fw.write_parquet(fp)
            fed.append(fw)
        cp.write_text(json.dumps(checks[d], indent=1, default=str))
        Bs.append(B)
        log.info("pass 2 %s in %.0f s", d, time.perf_counter() - t0)
        del S, P
        gc.collect()
    return pl.concat(Bs, how="diagonal_relaxed"), checks, (pl.concat(fed, how="diagonal_relaxed") if fed else pl.DataFrame())


FED_GRID = ({"h": 2.25}, {"h": 1.0})  # h = v/2 (the reward band's middle) and a tighter 1 ¢


def fed_rung(cfg: SimConfig, P: Dy.Prepared, S: dict, d: str, rk: dict) -> pl.DataFrame:
    """Rung 5 on the Fed December markets (the only recorded markets with a reward pool):
    quote at the best price inside the reward band, 200 shares (both pools' minimum), with the
    rewards our resting orders would have scored. Descriptive: five markets, no resolution in the
    sample (fills marked to the day's last fair value)."""
    yes = P.seg.filter(pl.col("family") == "fed_dec_2026")["yes"].unique().to_list()
    pools = RS.pools_for(cfg.data_dirs[0], yes)
    win = S["segments"].rename({"t0": "t_start", "t1": "t_last"}).select("yes", "t_start", "t_last")
    out = []
    for g in FED_GRID:
        prm = E.params(size=200.0, hyst=1.0, qmax=2000.0, **g)
        R = Dy.run_day(P, 5, prm, family="fed_dec_2026", lat_ms=X.LAT_BASE)
        rw = RS.reward_usd(R["orders"], cfg.fair_root / d, win, pools, fills=R["fills"]) if R["orders"].height else pl.DataFrame()
        a = A.attribute(R["fills"], S["fair"], R["segments"], rk.get("fed_dec_2026", 0.0))
        w = A.per_window(a, R["segments"], 200.0, rw)
        if rw.height:
            w = w.join(rw.select("yes", "pool", "mean_share", "minutes", "minutes_quoted"), on="yes", how="left")
        out.append(w.with_columns(pl.lit(d).alias("date"), pl.lit(g["h"]).alias("h")))
    return pl.concat(out, how="diagonal_relaxed")


# ------------------------------------------------------------------------------------------------
# report
# ------------------------------------------------------------------------------------------------
def report(cfg: SimConfig, dates: list[str], configs: list[dict], W: pl.DataFrame, sel: pl.DataFrame, B: pl.DataFrame,
           checks: dict, fed: pl.DataFrame) -> dict:
    T: dict[str, pl.DataFrame] = {}
    lock = dates[-1]
    tests = dates[1:-1]
    base = B.filter(pl.col("exp") == "band")
    T["rungs"] = X.window_stats(base.filter(pl.col("model") == X.MODEL_BASE), ["rung", "fold"])
    # robustness: only windows with no data gap (orders dropped at a gap would rest blind live)
    T["rungs_nogap"] = X.window_stats(base.filter((pl.col("model") == X.MODEL_BASE) & (pl.col("cover") >= 0.99)), ["rung", "fold"])
    T["band"] = X.window_stats(base, ["rung", "fold", "model"])
    T["latency"] = X.window_stats(B.filter(pl.col("exp") == "latency"), ["rung", "fold", "lat_ms"])
    T["by_day"] = X.window_stats(base.filter(pl.col("model") == X.MODEL_BASE), ["rung", "date"])
    T["noise"] = X.window_stats(B.filter(pl.col("exp").str.starts_with("noise") | ((pl.col("exp") == "band") & (pl.col("model") == X.MODEL_BASE)
                                                                                  & pl.col("rung").is_in([1, 3]))), ["rung", "fold", "exp"])
    T["grid_all"] = X.window_stats(W, ["rung", "cfg", "date"])
    T["selected"] = sel
    cf = pl.DataFrame([{k: (json.dumps(v) if isinstance(v, (list, dict)) else v) for k, v in c.items()} for c in configs], infer_schema_length=None)
    T["configs"] = cf
    # deflated Sharpe: per rung, the walk-forward test-day series; trials = every setting ever run
    # Why without day 0: it is training only and in-sample for its own fair-value fit and value table
    # (audit 8); its Sharpes would flatter the trial spread
    sr_days = dates[1:-1] or dates[:-1]
    sr_trials = X.window_stats(W.filter(pl.col("date").is_in(sr_days)), ["cfg"])["sharpe_w"].drop_nulls().to_numpy()
    dsr, gate, plat, need = {}, {}, {}, {}
    axes = {1: ["h", "hyst"], 2: ["h", "gamma", "end_s"], 3: ["h", "jump_c", "hold_ms", "gamma"], 4: ["v_thr", "gamma", "jump_c"], 0: ["h"]}
    for r in sorted(base["rung"].unique().to_list()):
        oos = base.filter((pl.col("rung") == r) & (pl.col("fold") == "test") & (pl.col("model") == X.MODEL_BASE))["pnl"].to_numpy()
        lb = base.filter((pl.col("rung") == r) & (pl.col("fold") == "lockbox"))
        if len(oos) > 2 and oos.std() > 0:
            sk, ku = X.moments(oos)
            dsr[r] = X.deflated_sharpe(float(oos.mean() / oos.std(ddof=1)), len(oos), sk, ku, sr_trials)
            need[r] = X.n_needed(float(oos.mean()), float(oos.std(ddof=1)))
        plat[r] = X.plateau(W, dates[:-1], r, axes[r], configs)
        pe_t = base.filter((pl.col("rung") == r) & (pl.col("fold") == "test") & (pl.col("model") == "pess"))["pnl"]
        pe_l = lb.filter(pl.col("model") == "pess")["pnl"]
        lo_t = float(pe_t.mean() - 1.96 * pe_t.std() / np.sqrt(pe_t.len())) if pe_t.len() > 1 else float("nan")
        crit = {"oos_pess_lower95_per_window": round(lo_t, 4), "oos_pess_positive_95": bool(lo_t > 0),
                "lockbox_pess_mean_positive": bool(pe_l.len() and float(pe_l.mean()) > 0),
                "dsr_ge_0.95": bool(dsr.get(r, {}).get("dsr") is not None and dsr[r]["dsr"] >= 0.95),
                "plateau": bool(plat[r].get("within_1se_share", 0) >= 0.5)}
        gate[r] = crit | {"verdict": "PASS" if all(v for k, v in crit.items() if isinstance(v, bool)) else "FAIL"}
    # delay check: does any rung make MORE money with more latency (beyond 2 SE)? that would be a bug
    lt = T["latency"].filter(pl.col("fold") == "test").sort("rung", "lat_ms")
    viol = []
    for r in lt["rung"].unique().to_list():
        x = lt.filter(pl.col("rung") == r)
        for a_, b_ in zip(x.iter_rows(named=True), list(x.iter_rows(named=True))[1:]):
            if b_["mean"] - a_["mean"] > 2 * max(a_["se"] or 0, b_["se"] or 0):
                viol.append({"rung": r, "from_ms": a_["lat_ms"], "to_ms": b_["lat_ms"], "gain": round(b_["mean"] - a_["mean"], 4)})
    nz = T["noise"].filter(pl.col("fold") == "test")
    noise_ok = {}
    for r in nz["rung"].unique().to_list():
        x = nz.filter(pl.col("rung") == r)
        b0 = x.filter(pl.col("exp") == "band")
        for e in ("noise_0.5", "noise_2.0"):
            y = x.filter(pl.col("exp") == e)
            if b0.height and y.height:
                noise_ok[f"{r}|{e}"] = {"true_fair": round(b0["mean"][0], 4), "noisy": round(y["mean"][0], 4),
                                        "noisy_not_better_2se": bool(y["mean"][0] - b0["mean"][0] <= 2 * max(b0["se"][0] or 0, y["se"][0] or 0))}
    if fed.height:
        T["fed"] = fed.group_by("date", "h").agg(*[pl.col(c).sum() for c in ("c1_spread", "c2_adverse", "c3_inventory", "c4_rebate", "c4_rewards", "pnl_hold", "pnl", "fills", "shares")],
                                                 pl.col("q_peak").max(), pl.col("mean_share").mean().alias("mean_share")).sort("date", "h")
    # Why the checks veto: a pass alongside a failed sanity check means the engine, not the
    # strategy, is the likelier explanation (audit 5)
    sane = (not viol) and all(v["noisy_not_better_2se"] for v in noise_ok.values()) and \
        all((c or {}).get("do_nothing_fills", 0) == 0 for c in checks.values())
    overall = "PASS" if sane and any(g["verdict"] == "PASS" for g in gate.values()) else "FAIL"
    res = {"dates": dates, "test_days": tests, "lockbox": lock, "settings": len(configs), "fair_mode": cfg.fair_mode, "engine": E.ENGINE_VERSION,
           "trials_logged": int(W.select("cfg", "date").unique().height), "size": X.SIZE, "lat_base_ms": X.LAT_BASE,
           "model_base": X.MODEL_BASE, "gate_b": overall, "gate_b_by_rung": gate, "dsr": dsr, "plateau": plat, "windows_needed": need,
           "delay_violations": viol, "noise_check": noise_ok, "sanity_ok": sane, "checks": checks}
    return {"T": T, "res": res}


def _fmt(df: pl.DataFrame) -> str:
    if not df.height:
        return "(empty)\n"
    cols = df.columns
    rows = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in df.iter_rows():
        rows.append("| " + " | ".join("" if v is None else (f"{v:.4g}" if isinstance(v, float) else str(v)) for v in r) + " |")
    return "\n".join(rows) + "\n"


def write(cfg: SimConfig, rep: dict) -> None:
    T, res = rep["T"], rep["res"]
    for k, v in T.items():
        v.write_parquet(cfg.out / f"table_{k}.parquet")
    (cfg.out / "sim.json").write_text(json.dumps(res, indent=1, default=str))
    params = {"grids": {str(k): v for k, v in X.GRIDS.items()}, "size": X.SIZE, "lat_base_ms": X.LAT_BASE, "lats": X.LATS,
              "models": X.MODELS, "jitter": Dy.JITTER, "selected": T["selected"].to_dicts()}
    (cfg.out / "sim_params.json").write_text(json.dumps(params, indent=1, default=str))
    keep = ["windows", "pnl", "mean", "se", "sharpe_w", "fills", "shares", "c1_spread", "c2_adverse", "c3_inventory", "c4_rebate", "worst_window", "q_peak"]
    S = [f"# Simulator v1 — summary (machine-written)\n", f"Days {res['dates']} · test {res['test_days']} · lockbox {res['lockbox']} · "
         f"{res['settings']} settings · size {res['size']} · base {res['model_base']} / {res['lat_base_ms']} ms. USD; mean/se per window.\n",
         f"## Gate B: **{res['gate_b']}**\n", "```\n" + json.dumps(res["gate_b_by_rung"], indent=1) + "\n```\n",
         "## Rungs (walk-forward selection, matched, 75 ms)\n", _fmt(T["rungs"].select("rung", "fold", *keep)),
         "## Rungs, gap-free windows only (robustness)\n", _fmt(T["rungs_nogap"].select("rung", "fold", "windows", "mean", "se", "pnl")),
         "## Fill-model band\n", _fmt(T["band"].select("rung", "fold", "model", "windows", "mean", "se", "pnl")),
         "## Latency\n", _fmt(T["latency"].select("rung", "fold", "lat_ms", "mean", "se", "fills")),
         "## By day\n", _fmt(T["by_day"].select("rung", "date", "windows", "mean", "se", "pnl", "fills")),
         "## Selected settings\n", _fmt(T["selected"]),
         "## Deflated Sharpe / plateau / windows needed\n", "```\n" + json.dumps({"dsr": res["dsr"], "plateau": res["plateau"], "need": res["windows_needed"]}, indent=1, default=str) + "\n```\n",
         "## Checks\n", "```\n" + json.dumps({"delay_violations": res["delay_violations"], "noise": res["noise_check"], "checks": res["checks"]}, indent=1, default=str) + "\n```\n"]
    if "fed" in T:
        S += ["## Fed rewards rung (descriptive)\n", _fmt(T["fed"])]
    (cfg.out / "summary.md").write_text("\n".join(S))


def run_sim(cfg: SimConfig) -> dict:
    dates = sorted(cfg.dates)
    cfg.out.mkdir(parents=True, exist_ok=True)
    fs_paths = fair_seconds(dates, cfg.fair_root, cfg.spot_root, cfg.out)
    ref = pl.read_parquet(cfg.fair_root / "reference" / "markets_ref.parquet")
    idx = market_index(ref)
    rk = rebate_k(fee_table(cfg.data_dirs))
    configs = [c for c in X.all_configs() if cfg.rungs is None or c["rung"] in cfg.rungs]
    W = pass1(cfg, dates, idx, fs_paths, rk, configs)
    sel = X.select(W, dates)
    B, checks, fed = pass2(cfg, dates, idx, fs_paths, rk, configs, sel)
    for d in dates:
        p = cfg.out / d / "checks.json"
        if p.exists():
            checks.setdefault(d, {})["inputs"] = json.loads(p.read_text())
    rep = report(cfg, dates, configs, W, sel, B, checks, fed)
    write(cfg, rep)
    if cfg.plots:
        from .plots import make_plots
        make_plots(rep["T"], rep["res"], cfg.plot_dir)
    return rep["res"]
