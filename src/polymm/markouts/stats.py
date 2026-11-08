"""Means with cluster-robust standard errors, the one ruler every markout table uses.

Why clustered: fills are not independent. One BTC path drives every fill in a 15-minute window,
and one taker order can print several fills at once. Treating 65k fills as 65k independent
draws would make SEs ~10x too small. A cluster = one 15m window (btc_15m) or one market-hour
(ladder, Fed), the same unit as the fair-value scores (D-023).

    mean = Σ w x / Σ w,   SE² = G/(G−1) · Σ_c (Σ_{i∈c} w_i (x_i − mean))² / (Σ w)²   (CR0 with small-G factor)

Why the analytic formula, not a bootstrap: identical to first order for G ≳ 30 clusters, and
fast enough to run inside every slice of every table.
"""
from __future__ import annotations

import math

import polars as pl


def grouped_mean(df: pl.DataFrame, cols: list[str], by: list[str], weight: str | None = None, cluster: str = "cluster") -> pl.DataFrame:
    """Long table: by…, col, n, n_clusters, mean, se. Each column uses its own non-null rows."""
    out = []
    w = pl.col(weight) if weight else pl.lit(1.0)
    key = by or ["_all"]
    if not by:
        df = df.with_columns(pl.lit(1).alias("_all"))
    for c in cols:
        d = df.filter(pl.col(c).is_not_null() & pl.col(c).is_finite()).select(*key, cluster, pl.col(c).alias("x"), w.alias("w"))
        if not d.height:
            continue
        g = d.group_by(key).agg(((pl.col("w") * pl.col("x")).sum() / pl.col("w").sum()).alias("mean"),
                                pl.col("w").sum().alias("W"), pl.len().alias("n"), pl.col(cluster).n_unique().alias("n_clusters"))
        e = (d.join(g.select(*key, "mean"), on=key).with_columns((pl.col("w") * (pl.col("x") - pl.col("mean"))).alias("e"))
             .group_by(*key, cluster).agg(pl.col("e").sum().alias("S")).group_by(key).agg((pl.col("S") ** 2).sum().alias("SS")))
        g = (g.join(e, on=key).with_columns(
            pl.when(pl.col("n_clusters") > 1)
              .then((pl.col("SS") * pl.col("n_clusters") / (pl.col("n_clusters") - 1)).sqrt() / pl.col("W"))
              .otherwise(None).alias("se"))
             .select(*key, pl.lit(c).alias("col"), "n", "n_clusters", "mean", "se"))
        out.append(g if by else g.drop("_all"))
    return pl.concat(out, how="diagonal_relaxed") if out else pl.DataFrame()


def wide(t: pl.DataFrame, by: list[str]) -> pl.DataFrame:
    """col → mean / se columns side by side (one row per group)."""
    if not t.height:
        return t
    m = t.pivot(on="col", index=by, values="mean")
    s = t.pivot(on="col", index=by, values="se").rename({c: c + "_se" for c in t["col"].unique().to_list()})
    n = t.group_by(by).agg(pl.col("n").max(), pl.col("n_clusters").max())
    return n.join(m, on=by).join(s, on=by).sort(by)


def z_ok(mean: float | None, se: float | None, z: float = 1.96) -> bool:
    return mean is not None and se is not None and not math.isnan(se) and mean - z * se > 0
