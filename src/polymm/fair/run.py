"""F7: `polymm fair` — the whole chain, walk-forward, and the report.

    reference (Gamma) ─┐
    per day: inputs ───┼─► candidates (mid, micro, Stoikov, BTC model + oracles, Fed)
                       │       │
                       │   walk-forward: fit Stoikov table and w on days < d, apply to d
                       │       │
                       └─► scores on held-out days only ─► tables, plots, summary.md
    final fit on all days ─► fair.parquet per day + fair_params.json (what step 7 reads)

Why walk-forward by day: the weights and the Stoikov table are fitted; scoring them on
the days they were fitted on would flatter them. Day 1 (11-06, laptop + history spot) is
training only; 11-07 and 11-08 are the test days.
"""
from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import polars as pl

from .blend import apply_w_smooth, fit_w_smooth, shrink_check, w_table
from .inputs import TAUS, build_inputs, check_inputs, measure_latency_ms
from .models import MODEL_COLS, add_anchor, add_book, add_fed, add_models, apply_stoikov, fit_stoikov, ladder_monotone
from .reference import build_reference, check_reference
from .score import fmt, score

log = logging.getLogger(__name__)

F15, FL, FF = "btc_15m", "btc_daily_ladder", "fed_dec_2026"
FEATS = {F15: ["f_min", "f_absd"], FL: ["f_lh", "f_absd"]}


@dataclass
class FairConfig:
    dates: list[str]
    root: Path = Path("reports/fair")
    spot_root: Path = Path("data_vps")
    data_dirs: list[Path] = field(default_factory=lambda: [Path("data"), Path("data_vps")])
    plot_dir: Path = Path("research/fair_value")
    fit_tau: int = 30
    refresh_ref: bool = True
    plots: bool = True


def regimes(g: pl.DataFrame) -> pl.DataFrame:
    absd = pl.col("d").abs()
    return g.with_columns(
        pl.when(pl.col("minute") <= 4).then(pl.lit("00-04")).when(pl.col("minute") <= 9).then(pl.lit("05-09"))
          .when(pl.col("minute") <= 12).then(pl.lit("10-12")).otherwise(pl.lit("13-14")).alias("min_b"),
        pl.when(absd < 0.5).then(pl.lit("<0.5")).when(absd < 1.5).then(pl.lit("0.5-1.5")).otherwise(pl.lit(">=1.5")).alias("absd_b"),
        pl.when(pl.col("tau") < 3600).then(pl.lit("<1h")).when(pl.col("tau") < 3 * 3600).then(pl.lit("1-3h"))
          .when(pl.col("tau") < 12 * 3600).then(pl.lit("3-12h")).otherwise(pl.lit(">=12h")).alias("hte_b"),
        ((pl.col("mid") >= 0.03) & (pl.col("mid") <= 0.97)).alias("active"),
        # smooth-weight features, roughly unit scale
        (pl.col("minute").cast(pl.Float64) / 14.0).alias("f_min"),
        (absd.clip(0, 3) / 3.0).alias("f_absd"),
        ((pl.col("tau") / 3600).log1p() / math.log1p(48)).alias("f_lh"),
    )


def candidates(g: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    g = add_book(g)
    g = add_models(g)
    g, fed_stats = add_fed(g)
    return regimes(g), fed_stats


HALF_LIVES = (2, 5, 10, 30, 120)  # s; the anchor's memory of the book–model offset
BOOK = "x_stoikov"  # the book's level: Stoikov beat the raw microprice at every horizon ≥ 5 s


def _anchor_all(g: pl.DataFrame, h: int) -> pl.DataFrame:
    """Anchored model and anchored oracles for one half-life (oracles: scoring only)."""
    for m in MODEL_COLS:
        if m in g.columns:
            g = add_anchor(g, BOOK, m, h, "x_anch" if m == "x_model" else m + "_a")
    return g


def _blend_loss(d: pl.DataFrame, beta: np.ndarray, feats: list[str], tau: int) -> float:
    b = apply_w_smooth(d, beta, feats, "x_anch", BOOK, "_xf").filter(pl.col(f"x_mid_f{tau}").is_not_null() & pl.col("_xf").is_finite())
    return float(((b[f"x_mid_f{tau}"] - b["_xf"]) ** 2).mean()) if b.height else float("inf")


def fit_params(train: pl.DataFrame, tau: int) -> dict:
    """Stoikov table, then per BTC family the anchor half-life and the smooth weight β, all
    on the training days only. h is chosen by the training loss of the full blend."""
    st = fit_stoikov(train)
    tr0 = apply_stoikov(train, st)
    betas, hls, losses = {}, {}, {}
    for fam, feats in FEATS.items():
        base = tr0.filter((pl.col("family") == fam) & pl.col("x_model").is_finite())
        if fam == FL:
            base = base.filter(pl.col("active"))
        if base.height < 1000:
            continue
        best = None
        for h in HALF_LIVES:
            tr = add_anchor(base, BOOK, "x_model", h, "x_anch")
            beta = fit_w_smooth(tr, "x_anch", BOOK, tau, feats)
            L = _blend_loss(tr, beta, feats, tau)
            losses[f"{fam}|{h}"] = L
            if best is None or L < best[0]:
                best = (L, h, beta)
        hls[fam], betas[fam] = best[1], best[2].tolist()
    return {"stoikov": st, "betas": betas, "half_life": hls, "train_loss": losses}


def apply_params(g: pl.DataFrame, params: dict) -> tuple[pl.DataFrame, dict]:
    g = apply_stoikov(g, params["stoikov"])
    parts = []
    for fam in g["family"].unique().to_list():
        p = g.filter(pl.col("family") == fam)
        if fam in params["betas"]:
            p = _anchor_all(p, params["half_life"][fam])
            p = apply_w_smooth(p, np.array(params["betas"][fam]), FEATS[fam], "x_anch", BOOK, "x_fair")
        elif fam == FF:
            p = p.with_columns(pl.lit(None, pl.Float64).alias("w_x_fair"), pl.coalesce(pl.col("x_fed"), pl.col(BOOK)).alias("x_fair"))
        else:
            p = p.with_columns(pl.lit(None, pl.Float64).alias("w_x_fair"), pl.col(BOOK).alias("x_fair"))
        parts.append(p)
    g = pl.concat(parts, how="diagonal_relaxed")
    g, mono = ladder_monotone(g, "x_fair", "x_fair_mono")
    g = g.with_columns(pl.when(pl.col("family") == FL).then(pl.col("x_fair_mono")).otherwise(pl.col("x_fair")).alias("x_fair")).drop("x_fair_mono")
    return g, mono


CANDS = {
    F15: ["x_mid", "x_micro", "x_stoikov", "x_model", "x_anch", "x_fair", "x_or_sigma_a", "x_or_basis_a", "x_or_zero_a", "x_or_lead1_a"],
    FL: ["x_mid", "x_micro", "x_stoikov", "x_model", "x_anch", "x_fair", "x_or_zero_a", "x_or_lead1_a"],
    FF: ["x_mid", "x_micro", "x_stoikov", "x_fed"],
}


def score_all(oos: pl.DataFrame) -> dict[str, pl.DataFrame]:
    T: dict[str, pl.DataFrame] = {}
    head, by_min, by_hte, by_absd = [], [], [], []
    for fam, cands in CANDS.items():
        d = oos.filter(pl.col("family") == fam)
        if fam != F15:
            d = d.filter(pl.col("active"))
        # Why a common sample: every candidate is scored on the same seconds, so a model
        # that is missing in hard moments (no σ yet, stale spot) cannot look better for it.
        need = "x_fed" if fam == FF else "x_model"
        d = d.filter(pl.col(need).is_not_null() & pl.col(need).is_finite())
        if not d.height:
            continue
        for c in cands:
            if c not in d.columns:
                continue
            s = score(d, c, by=["family"])
            head.append(s)
            if fam == F15:
                by_min.append(score(d, c, by=["family", "min_b"], taus=(5, 30)))
                by_absd.append(score(d, c, by=["family", "absd_b"], taus=(5, 30)))
            if fam == FL:
                by_hte.append(score(d, c, by=["family", "hte_b"], taus=(5, 30)))
    T["headline"] = pl.concat(head, how="diagonal_relaxed") if head else pl.DataFrame()
    T["by_minute_15m"] = pl.concat(by_min, how="diagonal_relaxed") if by_min else pl.DataFrame()
    T["by_absd_15m"] = pl.concat(by_absd, how="diagonal_relaxed") if by_absd else pl.DataFrame()
    T["by_hte_ladder"] = pl.concat(by_hte, how="diagonal_relaxed") if by_hte else pl.DataFrame()
    return T


def calibration(oos: pl.DataFrame, cols=("mid", "p_stoikov", "p_model", "p_anch", "p_fair")) -> pl.DataFrame:
    """15m: predicted probability vs share resolved Up, in 10 bins (all held-out seconds of
    resolved windows), plus the Brier score per column."""
    d = oos.filter((pl.col("family") == F15) & pl.col("outcome").is_not_null())
    out = []
    for c in cols:
        x = d.filter(pl.col(c).is_not_null()).with_columns((pl.col(c) * 10).floor().clip(0, 9).alias("bin"))
        t = x.group_by("bin").agg(pl.col(c).mean().alias("pred"), pl.col("outcome").mean().alias("obs"),
                                  pl.len().alias("n"), pl.col("cluster").n_unique().alias("windows")).sort("bin")
        out.append(t.with_columns(pl.lit(c).alias("col"),
                                  pl.lit(float(((x[c] - x["outcome"]) ** 2).mean())).alias("brier")))
    return pl.concat(out) if out else pl.DataFrame()


def run_fair(cfg: FairConfig) -> dict:
    t0 = time.perf_counter()
    R: dict = {"cfg": {"dates": cfg.dates, "fit_tau": cfg.fit_tau}}
    ref_path = cfg.root / "reference" / "markets_ref.parquet"
    ref = build_reference(cfg.data_dirs, ref_path) if cfg.refresh_ref or not ref_path.exists() else pl.read_parquet(ref_path)
    R["reference_checks"] = check_reference(ref, cfg.data_dirs)
    lat = measure_latency_ms(cfg.spot_root)
    R["binance_latency_ms"] = lat
    days = []
    R["input_checks"] = {}
    for d in cfg.dates:
        g = build_inputs(d, cfg.root / d, cfg.spot_root, ref, lat)
        R["input_checks"][d] = check_inputs(g)
        log.info("inputs %s: %s", d, R["input_checks"][d])
        days.append(g)
    G = pl.concat(days, how="diagonal_relaxed")
    G, R["fed_sum"] = candidates(G)
    # ---- walk-forward ----
    oos_parts = []
    R["folds"] = []
    for i in range(1, len(cfg.dates)):
        train = G.filter(pl.col("date").is_in(cfg.dates[:i]))
        test = G.filter(pl.col("date") == cfg.dates[i])
        params = fit_params(train, cfg.fit_tau)
        test, mono = apply_params(test, params)
        R["folds"].append({"test": cfg.dates[i], "train": cfg.dates[:i], "betas": params["betas"], "half_life": params["half_life"],
                          "train_loss": params["train_loss"], "ladder_mono": mono})
        oos_parts.append(test)
    oos = pl.concat(oos_parts, how="diagonal_relaxed").with_columns(
        [(1 / (1 + (-pl.col(c)).exp())).alias("p_" + c[2:]) for c in ("x_micro", "x_stoikov", "x_model", "x_anch", "x_fair")])
    T = score_all(oos)
    T["calibration_15m"] = calibration(oos)
    # ---- final fit on all days → series + params ----
    final = fit_params(G, cfg.fit_tau)
    Gf, R["ladder_mono_final"] = apply_params(G, final)
    # ---- descriptive (in-sample, all days): how fast the market closes the anchored gap ----
    wt = []
    for fam, by in ((F15, ["family", "min_b"]), (F15, ["family", "absd_b"]), (FL, ["family", "hte_b"])):
        d = Gf.filter((pl.col("family") == fam) & pl.col("x_anch").is_finite())
        if fam == FL:
            d = d.filter(pl.col("active"))
        for k in TAUS:
            if d.height:
                wt.append(w_table(d, "x_anch", BOOK, k, by).with_columns(pl.lit(k).alias("tau")))
    T["w_hat"] = pl.concat(wt, how="diagonal_relaxed") if wt else pl.DataFrame()
    R["shrink"] = {fam: {k: shrink_check(Gf.filter((pl.col("family") == fam) & (pl.col("active") | (pl.col("family") == F15))),
                                          "x_anch", BOOK, k) for k in TAUS} for fam in (F15, FL)}
    params_out = {"fit_tau": cfg.fit_tau, "dates": cfg.dates, "betas": final["betas"], "half_life_s": final["half_life"],
                  "book": BOOK, "features": FEATS, "binance_latency_ms": lat}
    (cfg.root / "fair_params.json").write_text(json.dumps(params_out, indent=2))
    final["stoikov"].write_parquet(cfg.root / "stoikov_table.parquet")
    sig = lambda c: (1 / (1 + (-pl.col(c)).exp()))
    for d in cfg.dates:
        Gf.filter(pl.col("date") == d).select(
            "date", "family", "event_slug", "condition_id", "asset_id", "sec", "mid", "micro",
            sig(BOOK).alias("p_book"), sig("x_model").alias("p_model"), sig("x_anch").alias("p_anch"),
            pl.col("w_x_fair").alias("w"), sig("x_fair").alias("p_fair"), "S", "K", "tau", "sigma_s", "d",
        ).write_parquet(cfg.root / d / "fair.parquet")
    for name, t in T.items():
        if t.height:
            t.write_parquet(cfg.root / f"table_{name}.parquet")
    R["oos_rows"] = oos.height
    R["seconds"] = round(time.perf_counter() - t0, 1)
    if cfg.plots:
        from .plots import make_plots
        try:
            make_plots(oos, T, G, cfg.plot_dir)
        except Exception:  # a broken figure must not cost the tables and the summary
            log.exception("plots failed")
    write_summary(R, T, cfg)
    return R


def write_summary(R: dict, T: dict, cfg: FairConfig) -> None:
    hc = ["cand", "tau", "n", "n_clusters", "skill", "skill_se", "b", "b_se", "r2", "hit", "brier", "brier_mid"]
    L = [f"# Fair value v1 — {', '.join(cfg.dates)}", "",
         f"Held-out days: {', '.join(cfg.dates[1:])} (each scored with parameters fitted on the days before it). "
         f"Fit horizon τ = {cfg.fit_tau} s. Binance latency (median receive − event) {R['binance_latency_ms']:.0f} ms. "
         f"Run time {R['seconds']} s.", "",
         "## Checks", "", f"- reference: `{R['reference_checks']}`"]
    for d, c in R["input_checks"].items():
        L.append(f"- inputs {d}: `{c}`")
    L += [f"- Fed sum of microprices: `{R['fed_sum']}`", f"- ladder monotonicity (final): `{R['ladder_mono_final']}`", ""]
    L += ["## Folds", ""] + [f"- test {f['test']} ← train {f['train']}: half-life `{f['half_life']}`, betas `{ {k: [round(x, 3) for x in v] for k, v in f['betas'].items()} }`, ladder `{f['ladder_mono']}`" for f in R["folds"]] + [""]
    head = T.get("headline", pl.DataFrame())
    for fam in (F15, FL, FF):
        if head.height:
            h = head.filter(pl.col("family") == fam)
            L += [f"## {fam} — held-out scores" + (" (active books: mid 0.03–0.97)" if fam != F15 else ""), "", fmt(h, [c for c in hc if c in h.columns])]
    for name in ("by_minute_15m", "by_absd_15m", "by_hte_ladder"):
        t = T.get(name, pl.DataFrame())
        if t.height:
            key = [c for c in ("min_b", "absd_b", "hte_b") if c in t.columns][0]
            L += [f"## {name} (skill, τ = 5 / 30 s)", "", fmt(t.filter(pl.col("cand").is_in(["x_micro", "x_stoikov", "x_model", "x_fair", "x_or_zero", "x_or_sigma"])),
                                                              ["cand", key, "tau", "n", "n_clusters", "skill", "skill_se", "b", "hit"])]
    w = T.get("w_hat", pl.DataFrame())
    if w.height:
        L += ["## ŵ(τ): share of the model–book gap the mid closes (in-sample, all days)", "",
              fmt(w.sort("family", "tau"), [c for c in ("family", "min_b", "absd_b", "hte_b", "tau", "w_hat", "w_se", "n", "n_clusters", "median_abs_gap") if c in w.columns])]
    L += ["## Shrinkage check (x_mid(t+τ) = α x_model + γ x_micro)", "", f"`{R['shrink']}`", ""]
    c = T.get("calibration_15m", pl.DataFrame())
    if c.height:
        L += ["## Calibration, btc_15m held-out (10 bins)", "", fmt(c, ["col", "bin", "pred", "obs", "n", "windows", "brier"])]
    (cfg.root / "summary.md").write_text("\n".join(L) + "\n")
