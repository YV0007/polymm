"""R3–R4: what it costs to be there, and the net value of each candidate quote.

R3  Cost of resting all day where the rewards are scored. From the stage-5 probes on the same
    market (virtual orders on the replayed book, D-026): per placement of a 200-share order, the
    shares filled before it stops and the 60 s markout of the fill incl. the maker rebate, in USD.
    A quote that rests all day is ~1440 such one-minute placements, so

        cost per day ≈ 1440 × E[filled_200 × (M60 + rebate)] / 100 × size / 200

    Why 200-share probes scaled linearly: the largest size the probes track; for bigger quotes the
    filled share of a taker's volume falls, so this overstates the cost (conservative). Why market ×
    hour clusters: the Fed family has only a handful of markets, so stage 5's per-market clusters
    give meaningless SEs (2 clusters); fills cluster by hour of news flow (stage 4's rule for slow
    families). Placement mapping: our quote at the best price = "touch", deeper = "behind" (an upper
    bound for 2¢+, which fill even less).

R4  net per day = rewards − cost; yield = net / capital, under the low / central / high competition
    bracket (rewards) and the four cancel models (cost). Event risk (the Fed meeting on 2026-12-09)
    is not in a sample that ends before it: stated, not modelled.
"""
from __future__ import annotations

import polars as pl

from ..markouts.fills import NS
from ..markouts.stats import grouped_mean

MODELS = ("pess", "matched", "opt")


def placement_cost(probes: pl.DataFrame, policy_cancel: dict | None = None) -> pl.DataFrame:
    """Per (yes, type, cancel model): USD per day per 200 shares resting, with SE (market × hour)."""
    from ..queue.analysis import apply_policy
    out = []
    for pol in ("passive", "stale"):
        P = apply_policy(probes, pol) if pol != "passive" else probes
        P = P.filter((pl.col("design") == "grid") & (pl.col("stop") != "rejected")).with_columns(
            (pl.col("yes") + "|" + (pl.col("t_dec") // (3600 * NS)).cast(pl.String)).alias("mh"))
        for m in MODELS:
            if f"filled200_{m}" not in P.columns:
                continue
            # Why drop fills without a 60 s markout (window or market ended first) rather than set
            # them to 0: stage 5 drops them too, and a 0 pulls the cost toward zero (audit 7)
            d = P.filter((pl.col(f"t_first_{m}") < 0) | pl.col(f"M60_fair_{m}").is_not_null()).with_columns(
                pl.when(pl.col(f"t_first_{m}") >= 0)
                  .then(pl.col(f"filled200_{m}") * (pl.col(f"M60_fair_{m}") + pl.col("rebate_c")) / 100)
                  .otherwise(0.0).alias("usd_per_placement"),
                pl.when(pl.col(f"t_first_{m}") >= 0).then(pl.col(f"filled200_{m}")).otherwise(0.0).alias("filled"))
            g = grouped_mean(d, ["usd_per_placement", "filled"], ["yes", "type"], cluster="mh")
            if g.height:
                out.append(g.with_columns(pl.lit(pol).alias("policy"), pl.lit(m).alias("model")))
    if not out:
        return pl.DataFrame()
    G = pl.concat(out)
    w = G.pivot(on="col", index=["yes", "type", "policy", "model"], values=["mean", "se", "n", "n_clusters"])
    return w.select("yes", "type", "policy", "model",
                    (pl.col("mean_usd_per_placement") * 1440).alias("usd_day_per200"),
                    (pl.col("se_usd_per_placement") * 1440).alias("usd_day_per200_se"),
                    (pl.col("mean_filled") * 1440).alias("shares_filled_day_per200"),
                    pl.col("n_usd_per_placement").alias("placements"), pl.col("n_clusters_usd_per_placement").alias("clusters"))


def net_value(shares: pl.DataFrame, cost: pl.DataFrame, pools: dict[str, float], by: list[str] | None = None) -> pl.DataFrame:
    """Per (yes, [by…], candidate): mean share under each bracket → rewards/day; cost/day by mapping
    the candidate's placement to touch/behind; net and yield per USD of capital.
    Why per day (by=["date"]): today's pool is applied to every recorded day, and 11-06's book was
    ~5× thinner than 11-07/08, so pooling the days roughly doubles the share (audit finding 2)."""
    by = by or []
    r = (shares.group_by("yes", *by, "cand", "quote", "size")
         .agg(pl.len().alias("minutes"), pl.col("capital").mean(), pl.col("share_low_comp").mean(), pl.col("share_central").mean(),
              pl.col("share_high_comp").mean(), pl.col("s_bid_c").mean(), pl.col("s_ask_c").mean(),
              pl.col("mid_adj").mean(), pl.col("comp_central").mean()))
    r = r.with_columns(pl.col("yes").replace_strict(pools, default=0.0).alias("pool"))
    r = r.with_columns([(pl.col("pool") * pl.col(c)).alias(f"reward_{k}") for c, k in
                        (("share_low_comp", "lowcomp"), ("share_central", "central"), ("share_high_comp", "highcomp"))])
    # placement type for the cost: "best" quotes rest at the touch, anything deeper is "behind"
    r = r.with_columns(pl.when(pl.col("quote").str.contains("best")).then(pl.lit("touch")).otherwise(pl.lit("behind")).alias("type"),
                       ((pl.col("s_bid_c").is_not_null()).cast(pl.Int8) + (pl.col("s_ask_c").is_not_null()).cast(pl.Int8)).alias("sides"))
    c = cost.select("yes", "type", "policy", "model", "usd_day_per200", "usd_day_per200_se")
    r = r.join(c, on=["yes", "type"], how="left").with_columns(
        (pl.col("usd_day_per200") * pl.col("size") / 200 * pl.col("sides")).alias("fill_pnl_day"),
        (pl.col("usd_day_per200_se") * pl.col("size") / 200 * pl.col("sides")).alias("fill_pnl_day_se"))
    r = r.with_columns((pl.col("reward_central") + pl.col("fill_pnl_day")).alias("net_central"),
                       (pl.col("reward_lowcomp") + pl.col("fill_pnl_day")).alias("net_lowcomp"),
                       (pl.col("reward_highcomp") + pl.col("fill_pnl_day")).alias("net_highcomp"))
    return r.with_columns((pl.col("net_central") / pl.col("capital")).alias("yield_central"),
                          (pl.col("net_highcomp") / pl.col("capital")).alias("yield_highcomp"),
                          (pl.col("net_lowcomp") / pl.col("capital")).alias("yield_lowcomp"),
                          (pl.col("reward_central") / pl.col("capital")).alias("gross_yield_central"))
