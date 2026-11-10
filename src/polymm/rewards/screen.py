"""R5: the screen over every market with a live reward pool.

For each sampled book (sample.py) we score a STANDARD quote and ask what its pool pays per USD of
capital:

    standard quote  two-sided; on each side join the best qualifying price (≥ min size) if it is
                    inside the max spread, else post at half the max spread from the adjusted mid
                    (joining a best price beyond v would score nothing)
    share           Q_min^us / (Q_min^us + Σ Q_min^others), others bracketed low / central / high
    gross yield     at a FIXED capital K (1k and 10k USD): q = K / capital per share,
                    reward = pool × q·k / (q·k + Σ Q_min^others), yield = reward / K per day.
                    Why fixed capital, not the minimum size: at the minimum size a tiny pool with no
                    competition shows hundreds of % a day on a few USD; at a fixed K a small or
                    crowded pool saturates exactly as it would for us.
    capacity        the capital at which our share would reach 25 %: beyond it we compete with
                    ourselves for the pool
    jumpiness       mean |Δ adjusted mid| between consecutive snapshots (¢ per 10 min)

Why gross: adverse selection can only be measured where we record the full book (stage 5); the Fed
check in R3/R4 shows its size where we can. The screen ranks where to look, not what we would earn.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl

from .rules import adjusted_mid, competing_bracket, q_min, score_weight, side_scores


def load_samples(data_root: Path, day: str | None = None) -> tuple[pl.DataFrame, str]:
    base = data_root / "reference" / "rewards_books"
    days = sorted(p.name[3:] for p in base.glob("dt=*"))
    day = day or (days[-1] if days else None)
    if day is None:
        return pl.DataFrame(), ""
    files = sorted((base / f"dt={day}").glob("snap=*.parquet"))
    return (pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed") if files else pl.DataFrame()), day


def standard_quote(S: pl.DataFrame, M: pl.DataFrame) -> pl.DataFrame:
    """Per (market, snapshot): our standard quote's share, reward/day, capital and capacity."""
    d = S.drop("tick", strict=False).join(M.select("yes_token", "condition_id", "question", "sector", "rate_per_day", "max_spread_c", "min_size",
                        "end_date", "taker_delay_s", "fee_rate", "rebate_rate", "tick"), on="yes_token", how="inner")
    d = d.filter(pl.col("mid_adj").is_not_null())
    rows = []
    for r in d.iter_rows(named=True):
        v, ms, mid = r["max_spread_c"], r["min_size"], r["mid_adj"]
        bp, bs = np.array(r["bid_px"] or [], dtype=float), np.array(r["bid_sz"] or [], dtype=float)
        ap, as_ = np.array(r["ask_px"] or [], dtype=float), np.array(r["ask_sz"] or [], dtype=float)
        bq = bp[bs >= ms]
        aq = ap[as_ >= ms]
        tick = float(r.get("tick") or 0.01)
        # Why fall back to the sample's best prices: the sampler keeps only levels within v + 1¢ of
        # the mid, so a qualifying best beyond that is not in the arrays; it would score nothing
        # anyway, and the step-in below handles it (v1 dropped these markets: 2,915 of them)
        bb = bq.max() if len(bq) else (r.get("best_bid") if r.get("best_bid") is not None else mid - 1.0)
        ba = aq.min() if len(aq) else (r.get("best_ask") if r.get("best_ask") is not None else mid + 1.0)
        spread_c = (ba - bb) * 100
        sb, sa = (mid - bb) * 100, (ba - mid) * 100
        pb, pa = bb, ba
        if sb >= v:   # best bid scores nothing: step in to v/2 below the mid (on the tick grid)
            pb = np.floor((mid - v / 200) / tick + 1e-9) * tick
            sb = (mid - pb) * 100
        if sa >= v:
            pa = np.ceil((mid + v / 200) / tick - 1e-9) * tick
            sa = (pa - mid) * 100
        # Why recompute the mid and the competition: a step-in order (≥ min size) moves the
        # size-cutoff mid, and one-sided step-ins move it most (audit 6b; score.py does the same)
        big = max(ms, 1.0)
        nbp, nbs = (np.append(bp, pb), np.append(bs, big)) if pb != bb else (bp, bs)
        nap, nas = (np.append(ap, pa), np.append(as_, big)) if pa != ba else (ap, as_)
        mid2 = adjusted_mid(nbp, nbs, nap, nas, ms) or mid
        sb, sa = (mid2 - pb) * 100, (pa - mid2) * 100
        k1, k2 = float(score_weight(v, sb)), float(score_weight(v, sa))
        k = q_min(k1, k2, mid2)  # our score per share of size (Q_min is homogeneous in size)
        cap_ps = pb + (1 - pa)
        c1, c2 = side_scores(bp, bs, ap, as_, mid2, v, ms)
        comp = dict(zip(("low", "central", "high"), competing_bracket(c1, c2, mid2)))
        out = {"yes_token": r["yes_token"], "snap_at": r["snap_at"], "mid": mid2, "spread_c": spread_c, "s_bid_c": sb, "s_ask_c": sa,
               "k_per_share": k, "capital_per_share": cap_ps, "stepped_in": bool(pb != bb or pa != ba)}
        for K in (1_000, 10_000):
            q = K / cap_ps if cap_ps > 0 else 0.0
            ours = q * k
            for tag in ("low", "central", "high"):
                share = ours / (ours + comp[tag]) if ours + comp[tag] > 0 else 0.0
                out[f"yield{K // 1000}k_{tag}"] = r["rate_per_day"] * share / K
        # share 25 %: q k / (q k + C) = 1/4 → q = C / (3 k). With no qualifying competition the
        # capacity is unbounded by others (None, flagged "uncontested"), not zero (audit 6a).
        out["uncontested"] = comp["central"] <= 0
        out["capacity_usd"] = (comp["central"] / (3 * k)) * cap_ps if (k > 0 and comp["central"] > 0) else None
        out["comp_central"] = comp["central"]
        rows.append(out)
    return pl.DataFrame(rows, infer_schema_length=None)


def screen(data_root: Path, day: str | None = None) -> tuple[pl.DataFrame, pl.DataFrame, dict]:
    S, day = load_samples(data_root, day)
    mfile = data_root / "reference" / "rewards" / f"dt={day}" / "markets.parquet"
    if not S.height or not mfile.exists():
        return pl.DataFrame(), pl.DataFrame(), {"day": day, "snapshots": 0}
    M = pl.read_parquet(mfile)
    Q = standard_quote(S, M)
    if not Q.height:
        return pl.DataFrame(), pl.DataFrame(), {"day": day, "snapshots": int(S["snap_at"].n_unique()), "markets_sampled": 0}
    # Why time to resolution at the last snapshot, not at run time (audit 6e)
    now = datetime.fromisoformat(str(S["snap_at"].max()))
    per = (Q.sort("snap_at").group_by("yes_token")
           .agg(pl.len().alias("snaps"), pl.col("mid").mean(), pl.col("spread_c").median(), pl.col("comp_central").median(),
                *[pl.col(f"yield{k}k_{t}").mean() for k in (1, 10) for t in ("low", "central", "high")], pl.col("capacity_usd").median(),
                pl.col("stepped_in").mean().alias("stepped_in_share"), pl.col("uncontested").mean().alias("uncontested_share"),
                (pl.col("mid").diff().abs() * 100).mean().alias("jump_c_10min"), pl.col("k_per_share").mean()))
    per = per.join(M.select("yes_token", "condition_id", "question", "sector", "rate_per_day", "max_spread_c", "min_size", "end_date",
                            "taker_delay_s", "fee_rate", "rebate_rate", "neg_risk"), on="yes_token", how="left")
    per = per.with_columns(
        ((pl.col("end_date").str.to_datetime(strict=False, time_zone="UTC") - pl.lit(now)).dt.total_hours() / 24).alias("days_to_end"),
        (~pl.col("mid").is_between(0.10, 0.90)).alias("two_sided_only"),
        # the daily payout floor is 1 USD: a market paying less at our size pays nothing (audit 6f)
        (pl.col("yield1k_central") * 1000 < 1.0).alias("below_floor_1k"))
    # Why ranked only on markets ≥ 1 day from resolution: a market resolving today is nearly
    # decided, every fill is informed, and its thin last-day competition inflates the yield.
    per = per.with_columns((pl.col("days_to_end") >= 1).alias("rankable"))
    R = per.filter(pl.col("rankable"))
    # yield columns: central = central competition; "cons" = the HIGH-competition bracket (lowest yield)
    sector = (R.group_by("sector")
              .agg(pl.len().alias("markets"), pl.col("rate_per_day").sum().alias("pool_usd_day"),
                   pl.col("yield1k_central").median().alias("yield1k_med"), pl.col("yield1k_central").quantile(0.75).alias("yield1k_p75"),
                   pl.col("yield1k_high").median().alias("yield1k_med_cons"), pl.col("yield10k_central").median().alias("yield10k_med"),
                   (pl.col("yield1k_high") > 0.002).sum().alias("markets_cons_over_0_2pct"),
                   pl.col("capacity_usd").median().alias("capacity_med_usd"), pl.col("capacity_usd").sum().alias("capacity_sum_usd"),
                   pl.col("jump_c_10min").median().alias("jump_med_c"), pl.col("days_to_end").median().alias("days_to_end_med"),
                   pl.col("two_sided_only").mean().alias("two_sided_only_share"), pl.col("below_floor_1k").mean().alias("below_floor_1k_share"),
                   (pl.col("uncontested_share") > 0.5).sum().alias("uncontested_markets"), pl.col("fee_rate").median())
              .sort("yield1k_med", descending=True))
    info = {"day": day, "snapshots": int(S["snap_at"].n_unique()), "markets_sampled": int(per.height),
            "markets_live": int(M.filter(pl.col("rate_per_day") > 0).height), "pool_usd_day": round(float(M["rate_per_day"].sum()), 1),
            "markets_rankable": int(R.height), "pool_rankable_usd_day": round(float(R["rate_per_day"].sum()), 1)}
    # Why count the drop-outs: a market with no qualifying two-sided book is either dead or one
    # where a single quoter would own the pool; say how many and how much they pay (audit 6d)
    live = M.filter(pl.col("rate_per_day") > 0)
    sampled_tokens = set(S["yes_token"].to_list())
    scored = set(per["yes_token"].to_list())
    nr = live.filter(~pl.col("yes_token").is_in(list(sampled_tokens)))
    nq = live.filter(pl.col("yes_token").is_in(list(sampled_tokens)) & ~pl.col("yes_token").is_in(list(scored)))
    info |= {"not_returned_markets": nr.height, "not_returned_pool_usd_day": round(float(nr["rate_per_day"].sum()), 1),
             "no_qualifying_book_markets": nq.height, "no_qualifying_book_pool_usd_day": round(float(nq["rate_per_day"].sum()), 1),
             "null_end_date_markets": int(per["end_date"].is_null().sum())}
    return per, sector, info
