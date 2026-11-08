"""A6: `polymm markouts` — fill table, markouts, signature, slices, model, Gate A, report.

    per day: trades + replay tob + strict windows + fair.parquet ─► fills (A0) ─► markouts (A1)
    all days ─► signature (A2), slices (A3)
    τ* fixed on the first day ─► walk-forward model on later days (A4) ─► Gate A (A5)
    ─► reports/markouts/{<date>/markouts.parquet, table_*.parquet, gate.json, summary.md}

Inputs are the fair-value day folders (`reports/fair/<date>`: replay/, clean_windows_strict,
fair.parquet), so markouts and fair value always describe the same books.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from . import analysis as A
from .engine import add_markouts
from .fills import build_fills, data_root, fee_table, load_tob, market_index

log = logging.getLogger(__name__)


@dataclass
class MarkoutConfig:
    dates: list[str]
    fair_root: Path = Path("reports/fair")
    out: Path = Path("reports/markouts")
    data_dirs: list[Path] = field(default_factory=lambda: [Path("data_vps"), Path("data")])
    plot_dir: Path = Path("research/adverse_selection")
    plots: bool = True
    reuse: bool = False  # reuse <out>/<date>/markouts.parquet (analysis-only rerun)
    spot_root: Path = Path("data_vps")
    fair_oos: bool = True  # walk-forward fair series (see fair_oos.py); False = reports/fair/<date>/fair.parquet


def build_day(date: str, cfg: MarkoutConfig, idx: pl.DataFrame, fees: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    dd = cfg.fair_root / date
    root = data_root(dd, cfg.data_dirs, date)
    tob = load_tob(dd, idx.filter(pl.col("oi") == 0)["yes"].unique().to_list())
    fp = (cfg.out / date / "fair_oos.parquet") if cfg.fair_oos else (dd / "fair.parquet")
    fair = pl.read_parquet(fp) if fp.exists() else None
    if fair is None:
        log.warning("%s: no fair series at %s; fair-reference markouts will be empty", date, fp)
    f, chk = build_fills(date, dd, root, idx, fees, tob, fair)
    f = add_markouts(f.filter(pl.col("clean")), tob, fair)
    # Why keep sign-inconsistent fills out of the statistics: if the trade did not print at the
    # prior touch on its stated side, either our pre-trade book or the side is off, and the
    # markout's sign is not trustworthy. 2.3 % on 11-07; the share is reported every run.
    chk["used"] = int(f["sign_ok"].sum())
    f = f.filter(pl.col("sign_ok"))
    chk["mid_vs_fair_M30_corr"] = round(f.select(pl.corr("M30_mid", "M30_fair")).item() or 0, 3)
    return f, chk


def run_markouts(cfg: MarkoutConfig) -> dict:
    t0 = time.perf_counter()
    cfg.out.mkdir(parents=True, exist_ok=True)
    ref = pl.read_parquet(cfg.fair_root / "reference" / "markets_ref.parquet")
    idx = market_index(ref)
    fees = fee_table(cfg.data_dirs)
    R: dict = {"dates": cfg.dates, "fees": fees.to_dicts(), "checks": {}}
    days = []
    if cfg.fair_oos and not (cfg.reuse and all((cfg.out / d / "fair_oos.parquet").exists() for d in cfg.dates)):
        from .fair_oos import build_fair_oos
        build_fair_oos(cfg.dates, cfg.fair_root, cfg.spot_root, cfg.out)
    for d in cfg.dates:
        mp, cp = cfg.out / d / "markouts.parquet", cfg.out / d / "checks.json"
        if cfg.reuse and mp.exists() and cp.exists():
            f, chk = pl.read_parquet(mp), json.loads(cp.read_text())
        else:
            f, chk = build_day(d, cfg, idx, fees)
            (cfg.out / d).mkdir(parents=True, exist_ok=True)
            f.write_parquet(mp)
            cp.write_text(json.dumps(chk, indent=1, default=str))
        R["checks"][d] = chk
        log.info("fills %s: %s", d, chk)
        days.append(f)
    F = pl.concat(days, how="diagonal_relaxed")
    T: dict[str, pl.DataFrame] = {}
    # A2
    T["signature"] = A.signature(F, ["family"])
    T["signature_size"] = A.signature(F, ["family"], weight="size")
    T["signature_day"] = A.signature(F, ["family", "date"])
    R["tau_star_info"] = {}
    tstar = A.tau_star(F.filter(pl.col("date") == cfg.dates[0]), info=R["tau_star_info"])
    R["tau_star"] = tstar
    # A3
    T["slices"] = A.slices(F, tstar)
    # own sample per horizon: a balanced panel would drop minute 14 entirely (no 60 s left in the window)
    T["minute_curve"] = A.grouped_mean(F.filter(pl.col("family") == "btc_15m"), ["h_fair", f"M{tstar['btc_15m']}_fair", "M60_fair"], ["minute"])
    # A4
    P, R["model"] = A.walk_forward(F, cfg.dates, tstar)
    if P.height:
        T["decision"] = A.decision_curve(P)
        T["calibration"] = A.calibration(P)
        T["model_metrics"] = A.model_metrics(P)
        P.write_parquet(cfg.out / "predictions.parquet")
    # the same model at 60 s for the BTC 15m family (τ* there is 1 s; 60 s is the horizon where
    # BTC information, not queue mechanics, drives the markout)
    P60, R["model_60"] = A.walk_forward(F.filter(pl.col("family") == "btc_15m"), cfg.dates, {"btc_15m": 60})
    if P60.height:
        T["decision_60"] = A.decision_curve(P60)
        T["model_metrics_60"] = A.model_metrics(P60)
    R["params_path"] = str(cfg.out / "as_params.json")
    (cfg.out / "as_params.json").write_text(json.dumps({"tau_star": tstar, "dates": cfg.dates, **A.final_params(F, tstar)}, indent=1))
    # A5
    G = A.gate(F, P, cfg.dates, tstar)
    R["verdict"], R["verdict_why"], R["verdict_family"] = A.verdict(G, tstar, cfg.dates[1:], fallback={f for f, i in R["tau_star_info"].items() if i.startswith("fallback")})
    for k, v in G.items():
        T["gate_" + k] = pl.DataFrame(v) if v else pl.DataFrame()
    for name, t in T.items():
        if t.height:
            t.write_parquet(cfg.out / f"table_{name}.parquet")
    R["fills_used"] = F.height
    R["seconds"] = round(time.perf_counter() - t0, 1)
    (cfg.out / "gate.json").write_text(json.dumps(R, indent=1, default=str))
    if cfg.plots:
        from .plots import make_plots
        try:
            make_plots(F, T, tstar, cfg.plot_dir)
        except Exception:  # a broken figure must not cost the tables and the summary
            log.exception("plots failed")
    write_summary(R, T, cfg)
    return R


def _fmt(t: pl.DataFrame, cols: list[str], nd: int = 2) -> str:
    cols = [c for c in cols if c in t.columns]
    if not t.height or not cols:
        return "_(empty)_"
    def f(v):
        if v is None:
            return "–"
        if isinstance(v, float):
            return f"{v:,.{nd}f}"
        if isinstance(v, int):
            return f"{v:,}"
        return str(v)
    L = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    L += ["| " + " | ".join(f(r[c]) for c in cols) + " |" for r in t.select(cols).iter_rows(named=True)]
    return "\n".join(L)


def write_summary(R: dict, T: dict, cfg: MarkoutConfig) -> None:
    L = [f"# Markouts and adverse selection v1 — {', '.join(cfg.dates)}", "",
         f"Fills used {R['fills_used']:,} · τ* (fixed on {cfg.dates[0]}) `{R['tau_star']}` `{R['tau_star_info']}` · run {R['seconds']} s. "
         "Units: cents per share, maker's view (+ = maker made money).", "",
         f"## Gate A verdict: **{R['verdict']}** · per family `{R['verdict_family']}`", ""] + [f"- {w}" for w in R["verdict_why"][:20]] + ["", "## Checks (A0)", ""]
    for d, c in R["checks"].items():
        L.append(f"- {d}: `{c}`")
    L += ["", f"Fees (Gamma): `{R['fees']}`", ""]
    s = T["signature"]
    for fam in A.FAMS:
        x = s.filter(pl.col("family") == fam)
        if not x.height:
            continue
        L += [f"## {fam}: signature (count-weighted)", "",
              _fmt(x.select("ref", "sample", "tau", "n", "n_clusters", "mean", "se", "col"), ["ref", "sample", "tau", "n", "n_clusters", "mean", "se"]), ""]
    for name, title, cols in (("gate_unconditional", "Gate A 1: unconditional π = M̄(τ*)", ["family", "scope", "tau", "n", "n_clusters", "h_fair", "M", "M_se", "M_reb", "M_reb_se", "M60", "M60_se"]),
                              ("gate_slices", "Gate A 2: slices selected on training days, scored on the next day", ["family", "test_day", "slice", "value", "metric", "train_n", "train_pi", "train_se", "test_n", "test_clusters", "test_pi", "test_se", "test_pi60", "test_se60"]),
                              ("gate_model", "Gate A 3: fills each scorer keeps (held-out, per day)", ["family", "test_day", "scorer", "metric", "kept_share", "n", "n_clusters", "pi", "se", "pi60", "se60"]),
                              ("model_metrics", "A4 model: out-of-sample fit of the edge at τ*", ["family", "scorer", "n", "r2_vs_const", "corr"]),
                              ("model_metrics_60", "A4 model: out-of-sample fit of the edge at 60 s (btc_15m)", ["family", "scorer", "n", "r2_vs_const", "corr"])):
        t = T.get(name, pl.DataFrame())
        L += [f"## {title}", "", _fmt(t, cols, 3 if name == "model_metrics" else 2), ""]
    for name, title in (("decision", "A4 decision curve at τ* (edge + rebate of kept fills, ¢)"), ("decision_60", "A4 decision curve at 60 s, btc_15m")):
        dc = T.get(name, pl.DataFrame())
        if dc.height:
            L += [f"## {title}", "",
                  _fmt(dc.filter(pl.col("drop").is_in([0.0, 0.1, 0.25, 0.5, 0.75, 0.9])).sort("family", "scorer", "drop"),
                       ["family", "scorer", "drop", "kept", "edge_c", "edge_size_c", "pnl_usd"]), ""]
    sl = T.get("slices", pl.DataFrame())
    if sl.height:
        L += ["## A3 slices at τ* (fair reference)", "",
              _fmt(sl, ["family", "slice", "ex_ante", "value", "n", "n_clusters", "h_fair", "M", "M_se", "a", "M_reb", "M_size"]), ""]
    L += ["## Model coefficients (per SD of each feature, cents)", "", f"`{R['model']}`", ""]
    (cfg.out / "summary.md").write_text("\n".join(L) + "\n")
