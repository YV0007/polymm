"""S3: what each fill earned, split into the four terms, and per-window / per-day P&L.

For fill i (side s, +1 = we bought YES; price P; q shares), fair value m at the start of the
taker order that hit us (stage 4/5's rule: the instant the information arrived), m^τ the fair
value τ seconds after the fill, V the settlement value:

    ① spread      s (m − P) q          what we captured against fair value
    ② adverse     s (m^τ − m) q        how far fair value moved against us by τ
    ③ inventory   s (V − m^τ) q        what holding the position to settlement added
    ④ rebate      r f P (1 − P) q      the maker rebate (+ liquidity rewards, reward markets)

① + ② + ③ = s (V − P) q exactly: the P&L of a fill held to settlement, which is what happens,
since the strategy never exits as a taker. Checked on every run (identity error).
V: the outcome (0/1) when the market resolved inside the data; otherwise (the Fed) the last fair
value of the day, i.e. marked to market. When t + τ is past the market's end, m^τ = V.
"""
from __future__ import annotations

import numpy as np
import polars as pl

from .inputs import NS, PK

TAUS = (1, 60)


def attribute(fills: pl.DataFrame, fair: pl.DataFrame, seg: pl.DataFrame, rebate_k: float, tau: int = 60) -> pl.DataFrame:
    """One row per fill with the four terms in USD."""
    if not fills.height:
        return pl.DataFrame()
    mk = seg.select("yes", "t_end_s", "outcome").unique("yes")
    last_fair = fair.group_by("yes").agg(pl.col("p").last().alias("p_last"))
    f = (fills.join(mk, on="yes", how="left").join(last_fair, on="yes", how="left")
         .with_columns((pl.col("pk") / PK).alias("P"),
                       pl.coalesce(pl.col("outcome").cast(pl.Float64), pl.col("p_last")).alias("V"),
                       pl.col("outcome").is_null().alias("marked"),
                       (pl.col("t") + tau * NS).alias("t_tau")))
    fr = fair.select("yes", "t", "p").sort("t")
    f = (f.sort("t_pre").join_asof(fr.rename({"t": "tp", "p": "m_pre"}), left_on="t_pre", right_on="tp", by="yes", strategy="backward")
         .sort("t").join_asof(fr.rename({"t": "tf", "p": "m_fill"}), left_on="t", right_on="tf", by="yes", strategy="backward")
         .sort("t_tau").join_asof(fr.rename({"t": "tt", "p": "m_tau_raw"}), left_on="t_tau", right_on="tt", by="yes", strategy="backward"))
    past_end = pl.col("t_tau") >= pl.col("t_end_s") * NS
    f = f.with_columns(pl.coalesce(pl.col("m_pre"), pl.col("m_fill")).alias("m"),
                       pl.when(past_end & ~pl.col("marked")).then(pl.col("V")).otherwise(pl.col("m_tau_raw")).alias("m_tau"))
    s, q = pl.col("s"), pl.col("qty")
    f = f.with_columns(
        (s * (pl.col("m") - pl.col("P")) * q).alias("c1_spread"),
        (s * (pl.col("m_tau") - pl.col("m")) * q).alias("c2_adverse"),
        (s * (pl.col("V") - pl.col("m_tau")) * q).alias("c3_inventory"),
        (rebate_k * pl.col("P") * (1 - pl.col("P")) * q).alias("c4_rebate"),
        (s * (pl.col("V") - pl.col("P")) * q).alias("pnl_hold"),
    )
    return f.drop("tp", "tf", "tt", "m_tau_raw").sort("yes", "t")


def identity_error(a: pl.DataFrame) -> float:
    """max |①+②+③ − s(V − P)q| over fills: must be ~0 (floating point)."""
    if not a.height:
        return 0.0
    e = (a["c1_spread"] + a["c2_adverse"] + a["c3_inventory"] - a["pnl_hold"]).abs()
    return float(e.max())


def per_window(a: pl.DataFrame, segs: pl.DataFrame, size: float, rewards: pl.DataFrame | None = None) -> pl.DataFrame:
    """One row per market (= one 15-minute window for BTC 15m): the four terms, total, fills,
    shares, peak inventory, capital. Windows we quoted but never filled count as zeros: a strategy
    that sits out is a result, not missing data.
    Capital (an upper bound, USD): peak |q| (a share costs ≤ 1) + both resting orders (2 × size)."""
    # cover: the share of the market's live window inside clean segments. < 1 means a data gap,
    # where our orders were dropped at the gap's start; live they would rest blind through it
    # (audit 4), so results are also reported on gap-free windows only.
    w = segs.group_by("yes", "condition_id").agg(pl.col("q_peak").max(), pl.col("decisions").sum(), pl.col("orders").sum(),
                                                 pl.col("rejected").sum(), pl.col("overflow").sum(), pl.col("t_start_s").first(),
                                                 pl.col("t_end_s").first(), (pl.col("t1") - pl.col("t0")).sum().alias("_live_ns"))
    w = w.with_columns(pl.when(pl.col("t_start_s").is_not_null())
                       .then(pl.col("_live_ns") / ((pl.col("t_end_s") - pl.col("t_start_s")) * NS)).otherwise(None).alias("cover")).drop("_live_ns")
    cols = ["c1_spread", "c2_adverse", "c3_inventory", "c4_rebate", "pnl_hold"]
    if a.height:
        g = a.group_by("yes").agg([pl.col(c).sum() for c in cols] + [pl.len().alias("fills"), pl.col("qty").sum().alias("shares"),
                                                                     pl.col("through").mean().alias("through_share")])
        w = w.join(g, on="yes", how="left")
    else:
        w = w.with_columns([pl.lit(0.0).alias(c) for c in cols] + [pl.lit(0).alias("fills"), pl.lit(0.0).alias("shares"),
                                                                   pl.lit(None, pl.Float64).alias("through_share")])
    w = w.with_columns([pl.col(c).fill_null(0.0) for c in cols] + [pl.col("fills").fill_null(0), pl.col("shares").fill_null(0.0)])
    w = w.with_columns(pl.lit(0.0).alias("c4_rewards"))
    if rewards is not None and rewards.height:
        w = w.drop("c4_rewards").join(rewards.select("yes", pl.col("reward_usd").alias("c4_rewards")), on="yes", how="left") \
             .with_columns(pl.col("c4_rewards").fill_null(0.0))
    return w.with_columns((pl.col("pnl_hold") + pl.col("c4_rebate") + pl.col("c4_rewards")).alias("pnl"),
                          (pl.col("q_peak") + 2 * size).alias("capital"))


def summarize(win: pl.DataFrame) -> dict:
    """Headline numbers of one run over its windows: totals, per-window mean and SE (the window is
    the unit of independence), Sharpe per window, worst window."""
    n = win.height
    if not n:
        return {"windows": 0}
    pnl = win["pnl"].to_numpy()
    mu, sd = float(pnl.mean()), float(pnl.std(ddof=1)) if n > 1 else float("nan")
    out = {"windows": n, "fills": int(win["fills"].sum()), "shares": round(float(win["shares"].sum()), 1),
           "pnl": round(float(pnl.sum()), 4), "pnl_per_window": round(mu, 5),
           "se_per_window": round(sd / np.sqrt(n), 5) if n > 1 else None,
           "sharpe_window": round(mu / sd, 4) if n > 1 and sd > 0 else None,
           "worst_window": round(float(pnl.min()), 4), "best_window": round(float(pnl.max()), 4),
           "q_peak": round(float(win["q_peak"].max()), 1),
           "capital_max": round(float(win["capital"].max()), 1)}
    for c in ("c1_spread", "c2_adverse", "c3_inventory", "c4_rebate", "c4_rewards"):
        out[c] = round(float(win[c].sum()), 4)
    sh = float(win["shares"].sum())
    if sh > 0:
        # per filled share in cents: comparable with stages 4–5
        for c in ("c1_spread", "c2_adverse", "c3_inventory", "c4_rebate"):
            out[c + "_c_per_share"] = round(100 * float(win[c].sum()) / sh, 4)
    return out
