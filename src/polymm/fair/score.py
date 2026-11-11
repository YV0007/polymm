"""F1: one ruler for every candidate fair value.

For a candidate x_c (log-odds, known at t) and horizon τ, with y = x_mid(t+τ) − x_mid(t)
and z = x_c(t) − x_mid(t):

    skill   1 − MSE_c / MSE_mid,  MSE_c = mean (x_mid(t+τ) − x_c(t))²
            Out of the box, nothing fitted: > 0 means the candidate is closer to where the
            mid goes than the mid itself. The headline number.
    a, b    OLS  y = a + b·z.  b ≈ 1: the mid closes the whole gap; b < 1: the candidate
            overreacts; b > 1: it underreacts. Standard error clustered by `cluster`.
    r2      R² of that regression (allows rescaling by b, so it is ≥ the skill).
    hit     among seconds where z ≠ 0 and the mid moved: share where it moved toward x_c.
    brier   mean (p_c − V)² against the resolved outcome V, next to the mid's own Brier.

Why clustered standard errors: all seconds of one 15-minute window share one BTC path,
so 900 rows are nowhere near 900 independent tests. Each cluster counts once.
Why log-odds for the errors: a 1¢ miss at p = 0.03 is a much bigger miss than at 0.50.
"""
from __future__ import annotations

import math

import polars as pl

from .inputs import TAUS

EPS = 1e-9


def _sigmoid(x: pl.Expr) -> pl.Expr:
    return 1 / (1 + (-x).exp())


def score(df: pl.DataFrame, cand: str, by: list[str] | None = None, taus=TAUS) -> pl.DataFrame:
    """Score column `cand` (log-odds) against future mids. `df` needs x_mid, x_mid_f{τ},
    cluster, outcome (may be null). Returns one row per group × τ."""
    by = list(by or [])
    grp = by if by else ["_g"]
    base = df if by else df.with_columns(pl.lit(1).alias("_g"))
    rows = []
    for k in taus:
        y = pl.col(f"x_mid_f{k}") - pl.col("x_mid")
        z = pl.col(cand) - pl.col("x_mid")
        d = (base.filter(pl.col(f"x_mid_f{k}").is_not_null() & pl.col(cand).is_not_null() & pl.col(cand).is_finite())
             .with_columns(y.alias("_y"), z.alias("_z"),
                           ((pl.col(f"x_mid_f{k}") - pl.col(cand)) ** 2).alias("_ec"),
                           ((pl.col(f"x_mid_f{k}") - pl.col("x_mid")) ** 2).alias("_em")))
        if not d.height:
            continue
        d = d.with_columns(pl.col("_z").mean().over(grp).alias("_zb"), pl.col("_y").mean().over(grp).alias("_yb"))
        d = d.with_columns((pl.col("_z") - pl.col("_zb")).alias("_zc"), (pl.col("_y") - pl.col("_yb")).alias("_yc"))
        agg = d.group_by(grp).agg(
            pl.len().alias("n"), pl.col("cluster").n_unique().alias("n_clusters"),
            (pl.col("_zc") * pl.col("_yc")).sum().alias("_szy"), (pl.col("_zc") ** 2).sum().alias("_szz"),
            (pl.col("_yc") ** 2).sum().alias("_syy"), pl.col("_zb").first(), pl.col("_yb").first(),
            pl.col("_ec").sum().alias("_sec"), pl.col("_em").sum().alias("_sem"),
            ((pl.col("_z").abs() > EPS) & (pl.col("_y").abs() > EPS)).sum().alias("n_dir"),
            ((pl.col("_z").abs() > EPS) & (pl.col("_y").abs() > EPS) & ((pl.col("_z") > 0) == (pl.col("_y") > 0))).sum().alias("_hits"),
            ((_sigmoid(pl.col(cand)) - pl.col("outcome")) ** 2).filter(pl.col("outcome").is_not_null()).mean().alias("brier"),
            ((pl.col("mid") - pl.col("outcome")) ** 2).filter(pl.col("outcome").is_not_null()).mean().alias("brier_mid"),
            pl.col("outcome").is_not_null().sum().alias("n_resolved"),
        )
        agg = agg.with_columns(
            (pl.col("_szy") / pl.col("_szz")).alias("b"),
            (pl.col("_sec") / pl.col("n")).alias("mse"),
            (1 - pl.col("_sec") / pl.col("_sem")).alias("skill"),
            (pl.col("_hits") / pl.col("n_dir")).alias("hit"),
        ).with_columns(
            (pl.col("_yb") - pl.col("b") * pl.col("_zb")).alias("a"),
            (1 - (pl.col("_syy") - pl.col("b") * pl.col("_szy")) / pl.col("_syy")).alias("r2"),
        )
        # cluster-robust SEs: b (sandwich) and skill (ratio estimator, delta method)
        d = d.join(agg.select(grp + ["b", "a", "skill"]), on=grp, how="left")
        cl = d.group_by(grp + ["cluster"]).agg(
            (pl.col("_zc") * (pl.col("_yc") - pl.col("b") * pl.col("_zc"))).sum().alias("_u"),
            (pl.col("_em") - pl.col("_ec")).sum().alias("_D"), pl.col("_em").sum().alias("_M"))
        se = cl.join(agg.select(grp + ["_szz", "skill", "_sem"]), on=grp).group_by(grp).agg(
            ((pl.col("_u") ** 2).sum().sqrt() / pl.col("_szz").first()).alias("b_se"),
            ((pl.col("_D") - pl.col("skill").first() * pl.col("_M")) ** 2).sum().sqrt()
            .truediv(pl.col("_sem").first()).alias("skill_se"))
        agg = agg.join(se, on=grp, how="left").with_columns(pl.lit(k).alias("tau"), pl.lit(cand).alias("cand"))
        rows.append(agg.drop([c for c in agg.columns if c.startswith("_")]))
    if not rows:
        return pl.DataFrame()
    out = pl.concat(rows, how="diagonal_relaxed")
    cols = ["cand"] + by + ["tau", "n", "n_clusters", "skill", "skill_se", "b", "b_se", "a", "r2", "hit", "n_dir",
                            "mse", "brier", "brier_mid", "n_resolved"]
    return out.select([c for c in cols if c in out.columns]).sort(by + ["tau"])



# ---- the same ruler, one day at a time (D-035) ------------------------------------------------
#
# `score` needs every scored second in memory at once. These two functions split it into a map
# step (`score_cells`, one small table per day) and a reduce step (`score_pooled`), so a day can be
# scored and dropped. A "cell" is one cluster × regime bucket × τ; per cell we keep
#     n, mean z, mean y, Σ(z−z̄)², Σ(y−ȳ)², Σ(z−z̄)(y−ȳ)   (centred within the cell)
#     Σ squared errors, hit counts, Brier sums              (plain sums)
# Why centred moments and not raw Σz², Σzy: the slope needs Σ(z−z̄)², and Σz² − n·z̄² cancels
# catastrophically when the gap z has a large mean (the raw model sits 0.35 log-odds off).
# Centred pieces combine exactly with Chan's parallel formula:
#     C_AB = C_A + C_B + n_A·n_B/n · (mean_A − mean_B)²   ≡   Σ_cells [C_i + n_i (m_i − m)²]
# Why per cluster: the clustered standard errors need each cluster's own sums, and a cluster
# (one 15m window or one market-hour) never spans two days, so per-day cells lose nothing.

_SUMS = ["sec", "sem", "n_dir", "hits", "bs", "bms", "n_resolved"]


def score_cells(df: pl.DataFrame, cand: str, keys: list[str], taus=TAUS) -> pl.DataFrame:
    """Sufficient statistics of `score` per (keys, cluster, τ) for one candidate on one day."""
    out = []
    for k in taus:
        d = (df.filter(pl.col(f"x_mid_f{k}").is_not_null() & pl.col(cand).is_not_null() & pl.col(cand).is_finite())
             .with_columns((pl.col(f"x_mid_f{k}") - pl.col("x_mid")).alias("_y"), (pl.col(cand) - pl.col("x_mid")).alias("_z"),
                           ((pl.col(f"x_mid_f{k}") - pl.col(cand)) ** 2).alias("_ec"),
                           ((pl.col(f"x_mid_f{k}") - pl.col("x_mid")) ** 2).alias("_em")))
        if not d.height:
            continue
        zc, yc = pl.col("_z") - pl.col("_z").mean(), pl.col("_y") - pl.col("_y").mean()  # centred within the cell
        res = pl.col("outcome").is_not_null()
        moved = (pl.col("_z").abs() > EPS) & (pl.col("_y").abs() > EPS)
        out.append(d.group_by(keys + ["cluster"]).agg(
            pl.len().alias("n"), pl.col("_z").mean().alias("mz"), pl.col("_y").mean().alias("my"),
            (zc ** 2).sum().alias("czz"), (yc ** 2).sum().alias("cyy"), (zc * yc).sum().alias("czy"),
            pl.col("_ec").sum().alias("sec"), pl.col("_em").sum().alias("sem"),
            moved.sum().alias("n_dir"), (moved & ((pl.col("_z") > 0) == (pl.col("_y") > 0))).sum().alias("hits"),
            ((_sigmoid(pl.col(cand)) - pl.col("outcome")) ** 2).filter(res).sum().alias("bs"),
            ((pl.col("mid") - pl.col("outcome")) ** 2).filter(res).sum().alias("bms"),
            res.sum().alias("n_resolved"),
        ).with_columns(pl.lit(k).alias("tau"), pl.lit(cand).alias("cand")))
    return pl.concat(out, how="diagonal_relaxed") if out else pl.DataFrame()


def _pool(t: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
    """Merge cells into `keys` groups: counts and sums add, centred moments by Chan's formula."""
    n = pl.col("n").cast(pl.Float64)
    N = n.sum().over(keys)
    t = t.with_columns(((n * pl.col("mz")).sum().over(keys) / N).alias("_MZ"),
                       ((n * pl.col("my")).sum().over(keys) / N).alias("_MY"))
    dz, dy = pl.col("mz") - pl.col("_MZ"), pl.col("my") - pl.col("_MY")
    return t.group_by(keys).agg(
        pl.col("n").sum(), pl.col("_MZ").first().alias("mz"), pl.col("_MY").first().alias("my"),
        (pl.col("czz") + n * dz ** 2).sum().alias("czz"), (pl.col("cyy") + n * dy ** 2).sum().alias("cyy"),
        (pl.col("czy") + n * dz * dy).sum().alias("czy"), *[pl.col(c).sum() for c in _SUMS])


def score_pooled(cells: pl.DataFrame, by: list[str]) -> pl.DataFrame:
    """`score` rebuilt from stacked `score_cells` (any number of days): same columns, same
    numbers up to floating-point rounding."""
    grp = ["cand", "tau"] + by
    C = _pool(cells, grp + ["cluster"])  # one row per cluster within each group
    A = _pool(C, grp).join(C.group_by(grp).agg(pl.len().alias("n_clusters")), on=grp, nulls_equal=True)
    A = A.with_columns(
        (pl.col("czy") / pl.col("czz")).alias("b"), (pl.col("sec") / pl.col("n")).alias("mse"),
        (1 - pl.col("sec") / pl.col("sem")).alias("skill"), (pl.col("hits") / pl.col("n_dir")).alias("hit"),
        pl.when(pl.col("n_resolved") > 0).then(pl.col("bs") / pl.col("n_resolved")).alias("brier"),
        pl.when(pl.col("n_resolved") > 0).then(pl.col("bms") / pl.col("n_resolved")).alias("brier_mid"),
    ).with_columns(
        (pl.col("my") - pl.col("b") * pl.col("mz")).alias("a"),
        (1 - (pl.col("cyy") - pl.col("b") * pl.col("czy")) / pl.col("cyy")).alias("r2"),
    )
    # clustered SEs: each cluster's score contribution, centred on the GROUP means
    g = A.select(grp + [pl.col("mz").alias("_MZ"), pl.col("my").alias("_MY"), "b", "skill",
                        pl.col("czz").alias("_SZZ"), pl.col("sem").alias("_SEM")])
    S = C.join(g, on=grp, nulls_equal=True)
    n, dz, dy = pl.col("n").cast(pl.Float64), pl.col("mz") - pl.col("_MZ"), pl.col("my") - pl.col("_MY")
    u = (pl.col("czy") + n * dz * dy) - pl.col("b") * (pl.col("czz") + n * dz ** 2)
    se = S.group_by(grp).agg(
        ((u ** 2).sum().sqrt() / pl.col("_SZZ").first()).alias("b_se"),
        (((pl.col("sem") - pl.col("sec")) - pl.col("skill") * pl.col("sem")) ** 2).sum().sqrt()
        .truediv(pl.col("_SEM").first()).alias("skill_se"))
    A = A.join(se, on=grp, nulls_equal=True).with_columns(
        pl.col("n").cast(pl.UInt32), pl.col("n_clusters").cast(pl.UInt32), pl.col("n_dir").cast(pl.UInt32),
        pl.col("n_resolved").cast(pl.UInt32))
    cols = ["cand"] + by + ["tau", "n", "n_clusters", "skill", "skill_se", "b", "b_se", "a", "r2", "hit", "n_dir",
                            "mse", "brier", "brier_mid", "n_resolved"]
    return A.select(cols).sort(by + ["tau"])


def fmt(t: pl.DataFrame, cols: list[str] | None = None) -> str:
    """Markdown table with sensible rounding (for reports)."""
    if not t.height:
        return "_no rows_"
    cols = cols or t.columns
    t = t.select(cols)

    def f(v, c):
        if v is None or (isinstance(v, float) and not math.isfinite(v)):
            return "–"
        if isinstance(v, float):
            if c in ("skill", "skill_se", "r2", "hit"):
                return f"{v:.3f}"
            if c in ("brier", "brier_mid", "mse"):
                return f"{v:.4f}"
            return f"{v:.3g}"
        if isinstance(v, int) and c in ("n", "n_dir", "n_resolved"):
            return f"{v:,}"
        return str(v)
    head = "| " + " | ".join(cols) + " |\n|" + "---|" * len(cols) + "\n"
    return head + "\n".join("| " + " | ".join(f(v, c) for v, c in zip(r, cols)) + " |" for r in t.iter_rows()) + "\n"
