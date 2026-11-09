"""Q0: the per-level event streams a virtual order lives in, and the checks that say whether
they can be trusted.

Polymarket publishes L2 only: after every book update, the new total size at a price. What we
can know per update at one level (token, side, price):

    cons   shares a taker consumed from the FRONT of the queue (the trades D-020 linked to this
           book decrease; applied at the decrease, because 80 % of decreases arrive ~18 ms
           BEFORE their trade message, so trade time would be late)
    res    the rest of the size change: > 0 someone joined (always behind us, time priority),
           < 0 someone cancelled (position in the line unknown: the θ problem)
    L      the level size after the update (others only; our virtual order is not in it)

Plus, per cancel, `addt`: pseudo-L3. If a cancel removes EXACTLY the size of an earlier add at
the same level (most recent unmatched one, within 60 s), it was very likely that order, and
`addt` is when it joined. A probe that arrived before `addt` knows that cancel was behind it;
one that arrived after knows it was ahead. Unmatched cancels stay ambiguous.

Prices are integer "pk" units of 0.001 (floats: 1 − 0.57 ≠ 0.43). Everything is on the YES
token: the NO book is its exact mirror (verified, DATA_PLAN §5), one queue per price.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
from numba import njit

from ..markouts.fills import NS, linked_deltas, load_tob, strict_windows, to_yes_equivalent

PK = 1000          # price units per 1.0 (0.001)
TICK = 10          # 1 cent in pk units; probes stay in 0.05–0.95 where the tick is 1 cent
MATCH_MAX_NS = 60 * NS


def _pk(e: pl.Expr) -> pl.Expr:
    return (e * PK).round(0).cast(pl.Int64)


def load_levels(day_dir: Path, yes: list[str]) -> pl.DataFrame:
    """One row per book update at one level of a YES token, sorted by (level, t)."""
    el = (pl.scan_parquet(str(day_dir / "replay" / "derived" / "events_labeled" / "**" / "*.parquet"))
          .filter(pl.col("asset_id").is_in(yes) & (pl.col("label") != "unchanged"))
          .select(pl.col("ts_recv_ns").alias("t"), pl.col("asset_id").alias("yes"),
                  pl.when(pl.col("side") == "BUY").then(1).otherwise(-1).cast(pl.Int8).alias("s"),
                  _pk(pl.col("price")).alias("pk"), pl.col("size_before").alias("Lb"), pl.col("size_after").alias("L"),
                  "size_change", "label", pl.col("trade_size_sum").fill_null(0.0).alias("tsum"), pl.col("trade_ts_recv_ns").alias("ttr"))
          .collect())
    # Why cons = linked trade volume and res = the remainder: a fill-labelled decrease can also
    # contain adds/cancels netted into the same update (19 % have a decrease smaller than the
    # trades). The taker still took `tsum` from the front; the rest is ordinary churn.
    el = el.with_columns(
        pl.when(pl.col("label") == "fill").then(pl.col("tsum")).otherwise(0.0).alias("cons"),
        pl.when(pl.col("label") == "fill").then(pl.col("size_change") + pl.col("tsum")).otherwise(pl.col("size_change")).alias("res"),
    ).drop("size_change", "tsum")
    el = el.sort("yes", "s", "pk", "t")
    lv = el.select("yes", "s", "pk").unique(maintain_order=True).with_row_index("level")
    return el.join(lv, on=["yes", "s", "pk"], how="left").sort("level", "t")


@njit(cache=True)
def _match(level, key, kind, t, max_age):
    """LIFO match of cancels to adds with the same (level, size). Rows sorted by (level, key, t).
    kind: 1 add, -1 cancel, 0 other. Returns, per row, the matched add's time (cancel rows) or -1."""
    n = len(t)
    out = np.full(n, -1, np.int64)
    stack = np.empty(n, np.int64)
    top = 0
    for i in range(n):
        if i == 0 or level[i] != level[i - 1] or key[i] != key[i - 1]:
            top = 0
        if kind[i] == 1:
            stack[top] = t[i]
            top += 1
        elif kind[i] == -1:
            # Why LIFO: young orders are cancelled far more often (80 % of added size goes
            # within 1 s), so the most recent same-size add is the likeliest owner.
            while top > 0 and t[i] - stack[top - 1] > max_age:
                top -= 1  # too old to trust; and everything below it is older still
            if top > 0:
                top -= 1
                out[i] = stack[top]
    return out


def add_pseudo_l3(el: pl.DataFrame) -> pl.DataFrame:
    """addt per cancel row (-1 = unmatched). Only pure adds/cancels take part: a residual inside a
    fill update is a netted sum of several orders, its size says nothing about one order."""
    pure = (el["cons"] == 0).to_numpy()
    res = el["res"].to_numpy()
    kind = np.where(pure & (res > 0), 1, np.where(pure & (res < 0), -1, 0)).astype(np.int8)
    key = np.round(np.abs(res) * 100).astype(np.int64)
    order = np.lexsort((el["t"].to_numpy(), key, el["level"].to_numpy()))
    m = _match(el["level"].to_numpy()[order], key[order], kind[order], el["t"].to_numpy()[order], MATCH_MAX_NS)
    addt = np.empty_like(m)
    addt[order] = m
    return el.with_columns(pl.Series("addt", addt))


def load_trades(root: Path, date: str, idx: pl.DataFrame, day_dir: Path) -> pl.DataFrame:
    """Every trade on the YES-equivalent book: maker side s (+1 maker bought YES = a bid was hit).

    t = the START of the taker order the trade belongs to: the earliest of its messages' receive
    times and the book decreases D-020 linked to them (stage 4's t_pre rule, D-024). Why: 80 % of
    book decreases arrive ~18 ms before the trade message, and a sweep's later trade messages
    arrive after its first decrease; timed at the message, a sweep already under way when our
    probe landed would fill it (phantom fill), and a cancel triggered by the sweep's own first
    decrease would seem to beat it (audit 2026-11-09, finding 2)."""
    tr = pl.read_parquet(str(root / "events" / "trades" / f"dt={date}" / "**" / "*.parquet")).join(idx, on="asset_id", how="inner")
    tr = to_yes_equivalent(tr)
    lk = linked_deltas(day_dir, idx)
    tr = (tr.join(lk, left_on=["ts_recv_ns", "yes"], right_on=["trade_ts_recv_ns", "yes"], how="left")
          .with_columns(pl.min_horizontal("ts_recv_ns", pl.col("t_delta").fill_null(pl.col("ts_recv_ns"))).alias("t_eff"))
          .with_columns(pl.col("t_eff").min().over("yes", "ts_exch_ms", "taker_dir").alias("t")))
    return tr.select("t", pl.col("ts_recv_ns").alias("t_recv"), "yes", pl.col("s").cast(pl.Int8), _pk(pl.col("P")).alias("pk"),
                     pl.col("size").alias("v"), "family").sort("yes", "s", "t")


def add_order_start(el: pl.DataFrame, tr: pl.DataFrame) -> pl.DataFrame:
    """tord per level update: for a fill update, the start of the taker order that consumed it (the
    instant to read h at); else the update's own time."""
    om = tr.group_by("yes", "t_recv").agg(pl.col("t").min().alias("tord"))
    # Why the row index: the kernel relies on the (level, t) order of updates, including ties
    # within one message; a join must not reshuffle it.
    el = el.with_row_index("_r").join(om, left_on=["yes", "ttr"], right_on=["yes", "t_recv"], how="left").sort("_r")
    return el.with_columns(pl.when(pl.col("cons") > 0).then(pl.min_horizontal("t", pl.col("tord").fill_null(pl.col("t"))))
                           .otherwise(pl.col("t")).alias("tord")).drop("ttr", "_r")


def load_book(day_dir: Path, yes: list[str]) -> pl.DataFrame:
    tob = load_tob(day_dir, yes)
    return tob.with_columns(_pk(pl.col("bid")).alias("bid_pk"), _pk(pl.col("ask")).alias("ask_pk")).sort("yes", "t")


def checks(el: pl.DataFrame, tr: pl.DataFrame, tob: pl.DataFrame) -> dict:
    """Q0 checks. (1) continuity: each update's size_before equals the previous size_after at
    that level (else the stream skipped something and x would drift); (2) how much traded volume
    is linked to a book decrease (unlinked volume is invisible to the queue: conservative);
    (3) pseudo-L3 match rate; (4) churn mix."""
    prev = el["L"].shift(1)
    same = el["level"] == el["level"].shift(1)
    brk = ((el["Lb"] - prev).abs() > 1e-6) & same
    n_cancel = int(((el["res"] < 0)).sum())
    pure_cancel = (el["res"] < 0) & (el["cons"] == 0)
    out = {
        "level_updates": el.height,
        "levels": int(el["level"].n_unique()),
        "continuity_break_share": round(float(brk.sum()) / max(int(same.sum()), 1), 5),
        "trade_volume": round(float(tr["v"].sum()), 1),
        "linked_volume_share": round(float(el["cons"].sum()) / max(float(tr["v"].sum()), 1e-9), 4),
        "add_volume": round(float(el.filter(pl.col("res") > 0)["res"].sum()), 1),
        "cancel_volume": round(float(-el.filter(pl.col("res") < 0)["res"].sum()), 1),
        "consumed_volume": round(float(el["cons"].sum()), 1),
        "cancels": n_cancel,
        "pseudo_l3_match_share": round(float((el["addt"] >= 0).sum()) / max(int(pure_cancel.sum()), 1), 4),
        "pseudo_l3_match_volume_share": round(float(-el.filter(pl.col("addt") >= 0)["res"].sum())
                                              / max(float(-el.filter(pure_cancel)["res"].sum()), 1e-9), 4),
    }
    # median lifetime of matched cancelled orders: the "flicker" in one number
    life = el.filter(pl.col("addt") >= 0).select(((pl.col("t") - pl.col("addt")) / 1e6).alias("ms"))["ms"]
    if life.len():
        out["matched_life_ms_p50"] = round(float(life.median()), 1)
        out["matched_life_ms_p90"] = round(float(life.quantile(0.9)), 1)
    return out


def touch_size_check(el: pl.DataFrame, tob: pl.DataFrame, n: int = 20000, seed: int = 0) -> dict:
    """At random top-of-book instants: does the level stream's size at the best bid / ask equal
    the replayed top of book's size? This is the x0 a back-of-queue probe would get."""
    t = tob.filter(pl.col("bid_pk").is_between(50, 950) & pl.col("ask_pk").is_between(50, 950)).sample(n=min(n, tob.height), seed=seed)
    lv = el.select("t", "yes", "s", "pk", "L").sort("t")
    res = {}
    for side, pkc, szc in ((1, "bid_pk", "bid_sz"), (-1, "ask_pk", "ask_sz")):
        q = (t.select("t", "yes", pl.col(pkc).alias("pk"), pl.col(szc).alias("sz")).sort("t")
             .join_asof(lv.filter(pl.col("s") == side), on="t", by=["yes", "pk"], strategy="backward"))
        ok = ((q["L"] - q["sz"]).abs() <= 1e-6)
        res["bid" if side == 1 else "ask"] = round(float(ok.mean()), 4)
    return {"touch_size_match": res}


def load_day(date: str, day_dir: Path, root: Path, idx: pl.DataFrame) -> dict:
    yes = idx.filter(pl.col("oi") == 0)["yes"].unique().to_list()
    tr = load_trades(root, date, idx, day_dir)
    el = add_order_start(add_pseudo_l3(load_levels(day_dir, yes)), tr)
    tob = load_book(day_dir, yes)
    sw = strict_windows(day_dir)
    chk = checks(el, tr, tob) | touch_size_check(el, tob)
    return {"levels": el, "trades": tr, "tob": tob, "windows": sw, "checks": chk}
