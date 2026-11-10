"""Rung 5: the liquidity rewards our simulated quotes would have earned (stage 6 rules, D-029).

The programme samples the book once a minute at a random second, scores every qualifying order,
normalises each minute to 1 and pays the day's pool by the average share. After a run we know
exactly which of our orders rested at each sampled second, so per sampled minute:

    our score    Q_min of OUR resting orders (price distance from the size-cutoff mid, size)
    competition  the bracketed Σ Q_min of everyone else (stage 6's central value)
    share        ours / (ours + competition)        reward = pool × mean share over the minutes

Minutes we did not quote count with share 0 (the pool is shared every minute, whether we are
there or not). The book at a sampled second is stage 6's rebuilt book (minute_books), kept only
where it matches the recorded top of book.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

from ..rewards.rules import adjusted_mid, competing_bracket, q_min, score_weight, side_scores
from ..rewards.score import minute_books
from .inputs import PK


def pools_for(data_root: Path, yes: list[str]) -> dict[str, tuple[float, float, float]]:
    """yes → (USD/day, max spread v in ¢, min size) from the latest reward-config snapshot.
    Why the latest for every day: only the current configuration is published (stage 6)."""
    snaps = sorted((data_root / "reference" / "rewards").glob("dt=*/markets.parquet"))
    if not snaps:
        return {}
    d = pl.read_parquet(snaps[-1]).filter(pl.col("yes_token").is_in(yes))
    return {r["yes_token"]: (float(r["rate_per_day"]), float(r["max_spread_c"]), float(r["min_size"])) for r in d.iter_rows(named=True)}


def reward_usd(orders: pl.DataFrame, day_dir: Path, windows: pl.DataFrame, pools: dict, seed: int = 7,
               fills: pl.DataFrame | None = None, day_minutes: int = 1440) -> pl.DataFrame:
    """(yes, minutes, minutes_quoted, mean_share, reward_usd) for every pooled token.
    Why the day's minutes as the denominator: a minute inside a data gap is a minute we were not
    quoting (share 0), not a missing observation; only minutes whose rebuilt book failed the
    top-of-book check are dropped as missing (audit 3: 11-07 covered 882 of 1440 minutes, and
    averaging over the covered ones alone overstated rewards 1.6×)."""
    yes = [y for y in pools if pools[y][0] > 0]
    if not yes:
        return pl.DataFrame()
    B = minute_books(day_dir, yes, windows, seed=seed)
    if not B.height:
        return pl.DataFrame()
    O = (orders.filter((pl.col("end") != 3) & pl.col("yes").is_in(yes))
         .select("yes", "seg", "oid", "t_arr", "t_end", "s", (pl.col("pk") / PK).alias("px"), "size")
         if orders.height else pl.DataFrame())
    # Why fills by time: an order's resting size at a sampled second is its size minus what had
    # filled BY then, not by its end (audit 7)
    Fq = (fills.select("yes", "seg", "oid", "t", "qty") if fills is not None and fills.height else
          pl.DataFrame(schema={"yes": pl.String, "seg": pl.Int64, "oid": pl.Int64, "t": pl.Int64, "qty": pl.Float64}))
    rows = []
    for (y, t), g in B.group_by(["yes", "t"], maintain_order=True):
        rate, v, ms = pools[y]
        b = g.filter(pl.col("side") == 1)
        a = g.filter(pl.col("side") == -1)
        bp, bs, ap, as_ = b["px"].to_numpy(), b["sz"].to_numpy(), a["px"].to_numpy(), a["sz"].to_numpy()
        ours = O.filter((pl.col("yes") == y) & (pl.col("t_arr") <= t) & (pl.col("t_end") > t)) if O.height else pl.DataFrame()
        if ours.height:
            done = (Fq.filter((pl.col("yes") == y) & (pl.col("t") <= t)).group_by("seg", "oid").agg(pl.col("qty").sum().alias("_f")))
            ours = (ours.join(done, on=["seg", "oid"], how="left").with_columns((pl.col("size") - pl.col("_f").fill_null(0.0)).alias("sz"))
                    .filter(pl.col("sz") > 0))
        ob = ours.filter(pl.col("s") == 1) if ours.height else ours
        oa = ours.filter(pl.col("s") == -1) if ours.height else ours
        nbp, nbs = (np.append(bp, ob["px"].to_numpy()), np.append(bs, ob["sz"].to_numpy())) if ours.height else (bp, bs)
        nap, nas = (np.append(ap, oa["px"].to_numpy()), np.append(as_, oa["sz"].to_numpy())) if ours.height else (ap, as_)
        mid = adjusted_mid(nbp, nbs, nap, nas, ms)
        if mid is None:
            continue
        o1 = o2 = 0.0
        if ours.height:
            q1 = ob.filter(pl.col("sz") >= ms)
            q2 = oa.filter(pl.col("sz") >= ms)
            o1 = float((score_weight(v, (mid - q1["px"].to_numpy()) * 100) * q1["sz"].to_numpy()).sum()) if q1.height else 0.0
            o2 = float((score_weight(v, (q2["px"].to_numpy() - mid) * 100) * q2["sz"].to_numpy()).sum()) if q2.height else 0.0
        us = q_min(o1, o2, mid)
        c1, c2 = side_scores(bp, bs, ap, as_, mid, v, ms)
        _, ce, _ = competing_bracket(c1, c2, mid)
        rows.append({"yes": y, "t": t, "quoted": ours.height > 0, "share": us / (us + ce) if us + ce > 0 else 0.0})
    if not rows:
        return pl.DataFrame()
    R = pl.DataFrame(rows)
    ok = B.select("yes", "tob_match_share").unique("yes")
    return (R.group_by("yes").agg(pl.len().alias("minutes"), pl.col("quoted").sum().alias("minutes_quoted"), pl.col("share").sum().alias("share_sum"))
            .join(ok, on="yes", how="left")
            # sampled-in-window minutes = kept / match share; the dropped ones are missing data
            .with_columns((pl.lit(float(day_minutes)) - (pl.col("minutes") / pl.col("tob_match_share").clip(1e-9, 1.0) - pl.col("minutes"))).alias("denom"))
            .with_columns((pl.col("share_sum") / pl.col("denom")).alias("mean_share"),
                          pl.col("yes").replace_strict({k: v[0] for k, v in pools.items()}, default=0.0).alias("pool"))
            .with_columns((pl.col("pool") * pl.col("mean_share")).alias("reward_usd")))
