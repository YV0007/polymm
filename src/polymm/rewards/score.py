"""R1–R2: the reward score of the book, minute by minute, and our share of it for a candidate quote.

R1  One random second in every minute (the programme samples at a random offset; the seed makes it
    reproducible), inside strict clean windows only: rebuild each scored level's size from the level
    stream (stage 5, events.load_levels: size after the last update at or before that instant),
    take the size-cutoff-adjusted mid, score every level, and bracket the competing Σ Q_min
    (rules.competing_bracket: L2 cannot say which maker owns which side).

R2  Insert our candidate quote into that same book (it can move the adjusted mid if it improves the
    price), score it exactly (we know our own sides), and divide by our score plus the competition:

        share_u = Q_min^us / (Q_min^us + Σ Q_min^others)     reward/day = pool × mean_u share_u

    Why the mean over minutes: the epoch normalises every minute to 1 and then the day to 1, so a
    maker's daily share is the average of its per-minute shares over the minutes anyone scored.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl

from ..markouts.fills import NS
from ..markouts.fills import load_tob
from ..queue.events import PK, load_levels
from .rules import adjusted_mid, competing_bracket, q_min, score_weight, side_scores


def minute_books(day_dir: Path, yes: list[str], windows: pl.DataFrame, seed: int = 7) -> pl.DataFrame:
    """Long table (yes, t, side, px, sz): every level with size > 0 at one random second per minute."""
    el = load_levels(day_dir, yes).select("t", "yes", "s", "pk", "L", "level")
    if not el.height:
        return pl.DataFrame()
    rng = np.random.default_rng(seed)
    w = windows.filter(pl.col("yes").is_in(yes))
    grids = []
    for r in w.iter_rows(named=True):
        m0, m1 = r["t_start"] // (60 * NS) + 1, r["t_last"] // (60 * NS)
        if m1 <= m0:
            continue
        mins = np.arange(m0, m1)
        t = mins * 60 * NS + rng.integers(0, 60 * NS, len(mins))
        grids.append(pl.DataFrame({"yes": r["yes"], "t": t}))
    if not grids:
        return pl.DataFrame()
    g = pl.concat(grids).unique(["yes", "t"]).sort("t")
    lv = el.select("yes", "s", "pk", "level").unique("level")
    # every (minute, level) pair of the token, then the level's size as of that second
    gl = g.join(lv, on="yes", how="inner").sort("t")
    gl = gl.join_asof(el.select("t", "level", "L").sort("t"), on="t", by="level", strategy="backward")
    B = (gl.filter(pl.col("L") > 0).select("yes", "t", pl.col("s").alias("side"), (pl.col("pk") / PK).alias("px"), pl.col("L").alias("sz"))
         .sort("yes", "t", "side", "px"))
    # Why check against the recorded top of book: the level stream has no rows for snapshot
    # loads, so a level set by a snapshot and never updated is missing and one removed during a
    # gap stays as a ghost (audit finding 5: 11-06's rebuilt best ask ≠ the recorded one in 98 % of
    # minutes). A minute is kept only if the rebuilt best bid and ask equal the replay's top of book.
    tob = load_tob(day_dir, yes).select("t", "yes", "bid", "ask").sort("t")
    best = B.group_by("yes", "t").agg(pl.col("px").filter(pl.col("side") == 1).max().alias("rb"),
                                      pl.col("px").filter(pl.col("side") == -1).min().alias("ra")).sort("t")
    chk = best.join_asof(tob, on="t", by="yes", strategy="backward").with_columns(
        (((pl.col("rb") - pl.col("bid")).abs() < 1e-9) & ((pl.col("ra") - pl.col("ask")).abs() < 1e-9)).fill_null(False).alias("ok"))
    keep = chk.filter("ok").select("yes", "t")
    share = chk.group_by("yes").agg(pl.col("ok").mean().round(4).alias("tob_match_share"))
    return B.join(keep, on=["yes", "t"], how="semi").join(share, on="yes", how="left")


@dataclass(frozen=True)
class Quote:
    """A candidate: distance from the adjusted mid in cents per side (None = no quote on that side;
    0 = join the best qualifying price), size in shares."""
    bid_c: float | None
    ask_c: float | None
    size: float

    @property
    def label(self) -> str:
        f = lambda x: "-" if x is None else ("best" if x == 0 else f"{x:g}c")
        return f"bid {f(self.bid_c)} / ask {f(self.ask_c)} × {self.size:g}"


def _place(mid: float, best: float, dist_c: float, side: int, tick: float, other_best: float) -> float | None:
    """Our price: dist 0 = join the best price; else mid ∓ dist rounded away from the mid to the
    tick, never crossing the opposite best (post-only)."""
    if dist_c == 0:
        p = best
    else:
        raw = mid - side * dist_c / 100
        p = (np.floor(raw / tick + 1e-9) if side == 1 else np.ceil(raw / tick - 1e-9)) * tick
    if (side == 1 and p >= other_best) or (side == -1 and p <= other_best) or not (0 < p < 1):
        return None
    return round(float(p), 6)


def score_minutes(books: pl.DataFrame, params: dict[str, tuple[float, float]], quotes: list[Quote], tick: dict[str, float]) -> pl.DataFrame:
    """One row per (yes, minute, candidate): the market's scores and, per candidate, our Q_min and
    share under the low / central / high competition bracket. params: yes → (v ¢, min size)."""
    out = []
    for (yes, t), g in books.group_by(["yes", "t"], maintain_order=True):
        v, ms = params[yes]
        tk = tick.get(yes, 0.01)
        b = g.filter(pl.col("side") == 1)
        a = g.filter(pl.col("side") == -1)
        bp, bs = b["px"].to_numpy(), b["sz"].to_numpy()
        ap, as_ = a["px"].to_numpy(), a["sz"].to_numpy()
        mid0 = adjusted_mid(bp, bs, ap, as_, ms)
        if mid0 is None:
            continue
        q1, q2 = side_scores(bp, bs, ap, as_, mid0, v, ms)
        lo, ce, hi = competing_bracket(q1, q2, mid0)
        base = {"yes": yes, "t": t, "mid_adj": mid0, "q_one": q1, "q_two": q2, "comp_low": lo, "comp_central": ce, "comp_high": hi}
        bb, ba = bp[bs >= ms].max(), ap[as_ >= ms].min()
        # post-only is checked against the best price of ANY size (a small level still crosses us)
        bb_any, ba_any = bp.max(), ap.min()
        for qi, q in enumerate(quotes):
            size = max(q.size, ms)
            pb = _place(mid0, bb, q.bid_c, 1, tk, ba_any) if q.bid_c is not None else None
            pa = _place(mid0, ba, q.ask_c, -1, tk, bb_any) if q.ask_c is not None else None
            # our order joins the book: it can move the size-cutoff mid if it improves a price
            nbp, nbs = (np.append(bp, pb), np.append(bs, size)) if pb is not None else (bp, bs)
            nap, nas = (np.append(ap, pa), np.append(as_, size)) if pa is not None else (ap, as_)
            mid = adjusted_mid(nbp, nbs, nap, nas, ms) or mid0
            o1 = float(score_weight(v, (mid - pb) * 100) * size) if pb is not None else 0.0
            o2 = float(score_weight(v, (pa - mid) * 100) * size) if pa is not None else 0.0
            ours = q_min(o1, o2, mid)
            c1, c2 = side_scores(bp, bs, ap, as_, mid, v, ms)
            clo, cce, chi = competing_bracket(c1, c2, mid)
            cap = (size * pb if pb is not None else 0.0) + (size * (1 - pa) if pa is not None else 0.0)
            out.append(base | {"cand": qi, "quote": q.label, "size": size, "bid_px": pb, "ask_px": pa, "mid_with_us": mid,
                               "s_bid_c": (mid - pb) * 100 if pb is not None else None, "s_ask_c": (pa - mid) * 100 if pa is not None else None,
                               "q_us": ours, "capital": cap,
                               "share_low_comp": ours / (ours + clo) if ours + clo > 0 else 0.0,
                               "share_central": ours / (ours + cce) if ours + cce > 0 else 0.0,
                               "share_high_comp": ours / (ours + chi) if ours + chi > 0 else 0.0})
    return pl.DataFrame(out)
