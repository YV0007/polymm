"""A1: the markout engine. For every fill and every horizon τ, the reference price τ later.

    h_r       = 100 · s · (m_r(t_pre) − P)            spread captured at the fill   [cents/share]
    M_r(τ)    = 100 · s · (m_r(t + τ) − P)            markout = realized spread
    a_r(τ)    = h_r − M_r(τ) = −100 · s · (m_r(t+τ) − m_r(t_pre))   adverse selection
    M_res     = 100 · s · (V_T − P)                   held to resolution (V_T ∈ {0, 1})

Two references r, both evaluated at the EXACT instants t_pre and t + τ:
    mid   event-level, from the replay top of book
    fair  exact mid + the fair value's premium over the mid, in log-odds, from the stage-3
          1 s grid at the last whole second at or before that instant:
              x_fair(t) = x_mid(t) + (x_fair − x_mid)_grid(⌊t⌋)
Why not the grid value itself: the grid row at ⌊t_pre⌋ is up to 1 s old (median 0.5 s), so the
book's own move in that second was booked as "adverse selection" and the fair horizon ran
τ + ~1 s instead of τ (audit 2026-11-08: +0.21 ¢ of spurious h on 11-07). The premium (what
the BTC model and the Stoikov table add to the book) moves slowly; the book part is exact.

Horizons stop at the earlier of the fill's strict clean-window end and the market's close
(null beyond): a stale book across a gap, or a post-close book pinned at 0.01/0.99, is not
what the market did while the quote could still trade.
"""
from __future__ import annotations

import polars as pl

from .fills import NS

TAUS = (1, 2, 5, 10, 30, 60, 120, 300, 900, 3600)  # 900/3600: only the slow families (ladder, Fed) reach them
REFS = ("mid", "fair")
CLIP = 1e-3


def _logit(e: pl.Expr) -> pl.Expr:
    q = e.clip(CLIP, 1 - CLIP)
    return (q / (1 - q)).log()


def _sigmoid(e: pl.Expr) -> pl.Expr:
    return 1 / (1 + (-e).exp())


def add_markouts(f: pl.DataFrame, tob: pl.DataFrame, fair: pl.DataFrame | None, taus=TAUS) -> pl.DataFrame:
    f = f.with_row_index("fid")
    cap = pl.min_horizontal("t_last", pl.col("t_end_s") * NS) if "t_end_s" in f.columns else pl.col("t_last")
    f = f.with_columns(cap.alias("_cap"), pl.col("mid").alias("m0_mid"))
    # ---- mid reference -----------------------------------------------------------------------
    mids = tob.select("t", "yes", "mid").sort("t")
    for k in taus:
        q = (f.select("fid", "yes", (pl.col("t_recv") + k * NS).alias("tq"), "_cap").sort("tq")
             .join_asof(mids.rename({"t": "tq"}), on="tq", by="yes", strategy="backward")
             .select("fid", pl.when(pl.col("tq") <= pl.col("_cap")).then(pl.col("mid")).otherwise(None).alias(f"m{k}_mid")))
        f = f.join(q, on="fid", how="left")
    # ---- fair reference: exact mid + grid premium ---------------------------------------------
    if fair is not None and fair.height:
        prem = fair.select(pl.col("asset_id").alias("yes"), "sec", (_logit(pl.col("p_fair")) - _logit(pl.col("mid"))).alias("dx"))
        f = (f.with_columns((pl.col("t_pre") // NS).alias("_sq"))
             .join(prem.rename({"sec": "_sq", "dx": "dx0"}), on=["yes", "_sq"], how="left")
             .with_columns(_sigmoid(_logit(pl.col("m0_mid")) + pl.col("dx0")).alias("m0_fair")).drop("_sq"))
        for k in taus:
            # Why floor: grid second sec holds the state at sec·NS, so ⌊t + τ⌋ is the latest premium
            # known at t + τ; the mid part is read at exactly t + τ.
            f = (f.with_columns(((pl.col("t_recv") + k * NS) // NS).alias("_sq"))
                 .join(prem.rename({"sec": "_sq", "dx": "_dx"}), on=["yes", "_sq"], how="left")
                 .with_columns(_sigmoid(_logit(pl.col(f"m{k}_mid")) + pl.col("_dx")).alias(f"m{k}_fair")).drop("_sq", "_dx"))
    else:
        f = f.with_columns(pl.lit(None, pl.Float64).alias("dx0"), *[pl.lit(None, pl.Float64).alias(f"m{k}_fair") for k in (0, *taus)])
    # ---- markout columns -----------------------------------------------------------------------
    cols = []
    for r in REFS:
        cols.append((100 * pl.col("s") * (pl.col(f"m0_{r}") - pl.col("P"))).alias(f"h_{r}"))
        for k in taus:
            cols.append((100 * pl.col("s") * (pl.col(f"m{k}_{r}") - pl.col("P"))).alias(f"M{k}_{r}"))
    f = f.with_columns(cols)
    f = f.with_columns([(pl.col(f"h_{r}") - pl.col(f"M{k}_{r}")).alias(f"a{k}_{r}") for r in REFS for k in taus])
    # resolution: only markets whose outcome Gamma reported (YES-equivalent V = outcome of the YES token)
    f = f.with_columns(
        pl.when(pl.col("outcome").is_not_null()).then(100 * pl.col("s") * (pl.col("outcome").cast(pl.Float64) - pl.col("P"))).otherwise(None).alias("M_res"),
        (100 * pl.col("rebate_ps")).alias("rebate_c"),
        (100 * pl.col("taker_fee_ps")).alias("fee_c"),
    )
    return f.drop("fid", "_cap")
