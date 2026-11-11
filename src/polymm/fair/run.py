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
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import polars as pl

from .blend import abs_gaps, apply_w_smooth, fit_w_smooth, shrink_part, shrink_pooled, w_cells, w_table_pooled
from .inputs import TAUS, day_bounds, measure_latency_ms
from .models import MODEL_COLS, add_anchor, add_book, add_fed, add_models, apply_stoikov, fed_stats, fed_sums, fit_stoikov, ladder_monotone
from .reference import build_reference, check_reference
from .score import fmt, score_cells, score_pooled

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


def fit_params(train: pl.DataFrame, tau: int, stoikov: pl.DataFrame | None = None) -> dict:
    """Stoikov table, then per BTC family the anchor half-life and the smooth weight β, all
    on the training days only. h is chosen by the training loss of the full blend.
    `stoikov`: the table already pooled from per-day sums (walk.py); then `train` only needs
    the BTC fit rows, not every row of every training day."""
    st = fit_stoikov(train) if stoikov is None else stoikov
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


def apply_params(g: pl.DataFrame, params: dict, warmup: pl.DataFrame | None = None) -> tuple[pl.DataFrame, dict]:
    """`warmup`: rows from just before `g` (the previous day's last hour) that only seed the
    anchor's EWMA and are dropped again before the ladder step. Why: applied to all days at once,
    the EWMA ran on across midnight; applied one day at a time it would restart at 00:00. With
    an hour of history the difference is ≤ 2^(−3600/120) ≈ 1e-9 of the offset (longest h = 120 s)."""
    if warmup is not None and warmup.height:
        # Why "vertical" without rechunk when the columns match (they do in run_fair): the frames are
        # just chained, no copy of the day; "diagonal_relaxed" copied it (+1.4 GB on a big day)
        same = warmup.schema == g.schema
        g = pl.concat([warmup.with_columns(pl.lit(True).alias("_wu")), g.with_columns(pl.lit(False).alias("_wu"))],
                      how="vertical" if same else "diagonal_relaxed", rechunk=False)
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
    if "_wu" in g.columns:
        g = g.filter(~pl.col("_wu")).drop("_wu")
    g, mono = ladder_monotone(g, "x_fair", "x_fair_mono")
    g = g.with_columns(pl.when(pl.col("family") == FL).then(pl.col("x_fair_mono")).otherwise(pl.col("x_fair")).alias("x_fair")).drop("x_fair_mono")
    return g, mono


CANDS = {
    F15: ["x_mid", "x_micro", "x_stoikov", "x_model", "x_anch", "x_fair", "x_or_sigma_a", "x_or_basis_a", "x_or_zero_a", "x_or_lead1_a"],
    FL: ["x_mid", "x_micro", "x_stoikov", "x_model", "x_anch", "x_fair", "x_or_zero_a", "x_or_lead1_a"],
    FF: ["x_mid", "x_micro", "x_stoikov", "x_fed"],
}


REGIME_KEYS = {F15: ["family", "min_b", "absd_b"], FL: ["family", "hte_b"], FF: ["family"]}  # finest buckets scored
CAL_COLS = ("mid", "p_stoikov", "p_model", "p_anch", "p_fair")


def scoring_sample(oos: pl.DataFrame, fam: str) -> pl.DataFrame:
    d = oos.filter(pl.col("family") == fam)
    if fam != F15:
        d = d.filter(pl.col("active"))
    # Why a common sample: every candidate is scored on the same seconds, so a model
    # that is missing in hard moments (no σ yet, stale spot) cannot look better for it.
    need = "x_fed" if fam == FF else "x_model"
    return d.filter(pl.col(need).is_not_null() & pl.col(need).is_finite())


def score_cells_day(oos: pl.DataFrame) -> pl.DataFrame:
    """Map step of the held-out scores: one held-out day → small per-cluster tables (score.py)."""
    out = []
    for fam, cands in CANDS.items():
        # Why select first: each candidate × τ filters the sample again; copying 15 columns
        # instead of ~50 keeps those copies small (the day is ~1–2 GB with all columns)
        need = ["family", "cluster", "mid", "outcome", "x_mid", *[f"x_mid_f{k}" for k in TAUS], *REGIME_KEYS[fam][1:]]
        d = scoring_sample(oos, fam)
        d = d.select(need + [c for c in cands if c in d.columns and c not in need])
        if not d.height:
            continue
        out += [score_cells(d, c, REGIME_KEYS[fam]) for c in cands if c in d.columns]
    out = [t for t in out if t.height]
    return pl.concat(out, how="diagonal_relaxed") if out else pl.DataFrame()


def score_all(cells: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """Reduce step: the held-out score tables from the stacked cells of all held-out days."""
    T: dict[str, pl.DataFrame] = {}
    head, by_min, by_hte, by_absd = [], [], [], []
    for fam, cands in CANDS.items():
        cf = cells.filter(pl.col("family") == fam) if cells.height else cells
        for c in cands:
            cc = cf.filter(pl.col("cand") == c) if cf.height else cf
            if not cc.height:
                continue
            head.append(score_pooled(cc, ["family"]))
            c2 = cc.filter(pl.col("tau").is_in([5, 30]))
            if fam == F15:
                by_min.append(score_pooled(c2, ["family", "min_b"]))
                by_absd.append(score_pooled(c2, ["family", "absd_b"]))
            if fam == FL:
                by_hte.append(score_pooled(c2, ["family", "hte_b"]))
    T["headline"] = pl.concat(head, how="diagonal_relaxed") if head else pl.DataFrame()
    T["by_minute_15m"] = pl.concat(by_min, how="diagonal_relaxed") if by_min else pl.DataFrame()
    T["by_absd_15m"] = pl.concat(by_absd, how="diagonal_relaxed") if by_absd else pl.DataFrame()
    T["by_hte_ladder"] = pl.concat(by_hte, how="diagonal_relaxed") if by_hte else pl.DataFrame()
    return T


def calibration_cells(oos: pl.DataFrame) -> pl.DataFrame:
    """Map step of the calibration: per column, bin and window the count and the sums."""
    d = oos.filter((pl.col("family") == F15) & pl.col("outcome").is_not_null())
    out = []
    for c in CAL_COLS:
        x = d.filter(pl.col(c).is_not_null()).with_columns((pl.col(c) * 10).floor().clip(0, 9).alias("bin"))
        out.append(x.group_by("bin", "cluster").agg(pl.len().alias("n"), pl.col(c).sum().alias("sp"), pl.col("outcome").sum().alias("so"),
                                                    ((pl.col(c) - pl.col("outcome")) ** 2).sum().alias("sb"))
                   .with_columns(pl.lit(c).alias("col")))
    return pl.concat(out, how="diagonal_relaxed")


def calibration(cells: pl.DataFrame) -> pl.DataFrame:
    """15m: predicted probability vs share resolved Up, in 10 bins (all held-out seconds of
    resolved windows), plus the Brier score per column. Built from `calibration_cells`."""
    out = []
    for c in CAL_COLS:
        x = cells.filter(pl.col("col") == c) if cells.height else cells
        if not x.height:
            continue
        t = x.group_by("bin").agg((pl.col("sp").sum() / pl.col("n").sum()).alias("pred"), (pl.col("so").sum() / pl.col("n").sum()).alias("obs"),
                                  pl.col("n").sum().cast(pl.UInt32), pl.col("cluster").n_unique().cast(pl.UInt32).alias("windows")).sort("bin")
        out.append(t.with_columns(pl.lit(c).alias("col"), pl.lit(float(x["sb"].sum() / x["n"].sum())).alias("brier")))
    return pl.concat(out) if out else pl.DataFrame()


PLOT_COLS = ["family", "condition_id", "event_slug", "sec", "t_start_s", "mid", "x_model", "p_model", "p_anch", "p_fair",
             "S", "K", "basis0", "outcome"]


def plot_windows_day(oos: pl.DataFrame, k: int = 3) -> pl.DataFrame:
    """The report's example window is the 3rd-largest model–mid gap among held-out 15m windows
    with ≥ 800 s (plots.py). The overall top 3 are always among each day's top 3, so a day only
    has to keep those three windows' rows (~2,700) instead of all its seconds."""
    e = oos.filter((pl.col("family") == F15) & pl.col("x_model").is_finite())
    pick = (e.group_by("condition_id").agg(pl.len().alias("n"), (pl.col("p_model") - pl.col("mid")).abs().max().alias("gap"))
            .filter(pl.col("n") >= 800).sort("gap", descending=True).head(k))
    return e.filter(pl.col("condition_id").is_in(pick["condition_id"].implode())).select(PLOT_COLS)


def _sum_dicts(a: dict, b: dict) -> dict:
    return {k: a.get(k, 0) + b.get(k, 0) for k in {**a, **b}}


def run_fair(cfg: FairConfig) -> dict:
    """Two passes over the days, one day in memory at a time (D-035):
        1. walk-forward (walk.py): each held-out day is scored with parameters from the days
           before it and reduced to small per-cluster sums; the day's grid is cached on disk
        2. final fit on all days (from the fit store), then each cached day is re-read, given
           the final fair value, written to fair.parquet and reduced to ŵ / shrink sums."""
    from .walk import walk_forward  # Why here: walk.py imports this module (fit_params, ...)

    t0 = time.perf_counter()
    R: dict = {"cfg": {"dates": cfg.dates, "fit_tau": cfg.fit_tau}}
    ref_path = cfg.root / "reference" / "markets_ref.parquet"
    ref = build_reference(cfg.data_dirs, ref_path) if cfg.refresh_ref or not ref_path.exists() else pl.read_parquet(ref_path)
    R["reference_checks"] = check_reference(ref, cfg.data_dirs)
    lat = measure_latency_ms(cfg.spot_root)
    R["binance_latency_ms"] = lat
    R["input_checks"], R["folds"] = {}, []
    cells, cal, plot_rows, fed = [], [], [], []
    n_oos = 0
    sig = lambda c: (1 / (1 + (-pl.col(c)).exp()))
    # Why a cache on disk: pass 2 needs every day's grid again, after the final fit; re-reading
    # a parquet (~seconds) is cheaper than rebuilding the inputs (~a minute a day), and the
    # temp folder is deleted when the run ends, crashed or not.
    with tempfile.TemporaryDirectory(prefix=".wf_cache_", dir=cfg.root) as tmp:
        cache = Path(tmp)

        def on_day(day) -> None:
            nonlocal n_oos
            R["input_checks"][day.date] = day.checks
            log.info("inputs %s: %s", day.date, day.checks)
            fed.append((day.date, *fed_sums(day.grid)))
            day.grid.write_parquet(cache / f"{day.date}.parquet")
            if day.fair is None:
                return
            R["folds"].append({"test": day.date, "train": day.train, "betas": day.params["betas"], "half_life": day.params["half_life"],
                               "train_loss": day.params["train_loss"], "ladder_mono": day.mono})
            oos = day.fair.with_columns([sig(c).alias("p_" + c[2:]) for c in ("x_micro", "x_stoikov", "x_model", "x_anch", "x_fair")])
            cells.append(score_cells_day(oos))
            cal.append(calibration_cells(oos))
            plot_rows.append(plot_windows_day(oos))
            n_oos += oos.height

        store = walk_forward(cfg.dates, cfg.root, cfg.spot_root, cfg.fit_tau, on_day, first_day="skip", ref=ref, latency_ms=lat)
        # Why per-day market counts: Σp = 1 is checked per day now; one number if every day had the same Fed markets
        n_mk = {d: n for d, n, _, _ in fed}
        R["fed_sum"] = {} if not any(n_mk.values()) else fed_stats(
            next(iter(n_mk.values())) if len(set(n_mk.values())) == 1 else n_mk,
            pl.concat([f for _, _, f, _ in fed]), sum(s for *_, s in fed))
        cells = [c for c in cells if c.height]
        T = score_all(pl.concat(cells, how="diagonal_relaxed") if cells else pl.DataFrame())
        T["calibration_15m"] = calibration(pl.concat(cal, how="diagonal_relaxed") if cal else pl.DataFrame())
        # ---- final fit on all days → series + params ----
        log.info("final fit on %s (fit store %.0f MB)", store.dates, store.nbytes() / 2**20)
        final = store.fit()
        del store
        # ---- pass 2: final series per day; descriptive (in-sample) ŵ and shrink sums ----
        WBY = {F15: (["family", "min_b"], ["family", "absd_b"]), FL: (["family", "hte_b"],)}
        W = {(fam, tuple(by), k): [] for fam, bys in WBY.items() for by in bys for k in TAUS}
        GAPS = {fam: [] for fam in WBY}
        SH = {(fam, k): [] for fam in (F15, FL) for k in TAUS}
        R["ladder_mono_final"] = {}
        tail = None
        for d in cfg.dates:
            g = pl.read_parquet(cache / f"{d}.parquet")
            Gf, mono = apply_params(g, final, warmup=tail)
            R["ladder_mono_final"] = _sum_dicts(R["ladder_mono_final"], mono)
            # the next day's EWMA warm-up: this day's last hour (inputs only; apply recomputes the rest)
            tail = g.filter(pl.col("family").is_in(list(final["betas"])) & (pl.col("sec") >= day_bounds(d)[1] - 3600))
            del g
            # Why sink (lazy) and not select().write_parquet: the select would first build the whole
            # 18-column output for the day in memory; sink_parquet computes and writes it in batches
            Gf.lazy().select(
                "date", "family", "event_slug", "condition_id", "asset_id", "sec", "mid", "micro",
                sig(BOOK).alias("p_book"), sig("x_model").alias("p_model"), sig("x_anch").alias("p_anch"),
                pl.col("w_x_fair").alias("w"), sig("x_fair").alias("p_fair"), "S", "K", "tau", "sigma_s", "d",
            ).sink_parquet(cfg.root / d / "fair.parquet")
            # Why: ŵ and the shrink check need 13 of the ~50 columns; keep only those for the rest of the day
            Gf = Gf.select("family", "active", "cluster", "min_b", "absd_b", "hte_b", "x_anch", BOOK, *[f"x_mid_f{k}" for k in TAUS])
            for fam, bys in WBY.items():
                x = Gf.filter((pl.col("family") == fam) & pl.col("x_anch").is_finite())
                if fam == FL:
                    x = x.filter(pl.col("active"))
                if not x.height:
                    continue
                GAPS[fam].append(abs_gaps(x, "x_anch", BOOK, sorted({b for by in bys for b in by}), TAUS))
                for by in bys:
                    for k in TAUS:
                        W[(fam, tuple(by), k)].append(w_cells(x, "x_anch", BOOK, k, by))
            for (fam, k), parts in SH.items():
                parts.append(shrink_part(Gf.filter((pl.col("family") == fam) & (pl.col("active") | (pl.col("family") == F15))), "x_anch", BOOK, k))
            log.info("final series %s: %d rows", d, Gf.height)
            del Gf
    wt = [w_table_pooled(pl.concat(wc), pl.concat(GAPS[fam]), list(by), k).with_columns(pl.lit(k).alias("tau"))
          for (fam, by, k), wc in W.items() if wc]
    T["w_hat"] = pl.concat(wt, how="diagonal_relaxed") if wt else pl.DataFrame()
    R["shrink"] = {fam: {k: shrink_pooled(SH[(fam, k)]) for k in TAUS} for fam in (F15, FL)}
    params_out = {"fit_tau": cfg.fit_tau, "dates": cfg.dates, "betas": final["betas"], "half_life_s": final["half_life"],
                  "book": BOOK, "features": FEATS, "binance_latency_ms": lat}
    (cfg.root / "fair_params.json").write_text(json.dumps(params_out, indent=2))
    final["stoikov"].write_parquet(cfg.root / "stoikov_table.parquet")
    for name, t in T.items():
        if t.height:
            t.write_parquet(cfg.root / f"table_{name}.parquet")
    R["oos_rows"] = n_oos
    R["seconds"] = round(time.perf_counter() - t0, 1)
    if cfg.plots and plot_rows:
        from .plots import make_plots
        try:
            make_plots(pl.concat(plot_rows), T, None, cfg.plot_dir)
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
