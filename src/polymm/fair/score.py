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
