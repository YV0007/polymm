"""The programme rules as published (docs read 2026-11-10), as pure functions.

Liquidity rewards (docs.polymarket.com/market-makers/liquidity-rewards):

    S(v, s)  = ((v − s) / v)²              order score; v = max qualifying spread (¢), s = distance
                                            from the size-cutoff-adjusted midpoint (¢); 0 beyond v
    Q_one    = Σ S·size over bids           (the YES book: a NO ask at 1 − p is a YES bid at p)
    Q_two    = Σ S·size over asks
    Q_min    = max(min(Q_one, Q_two), max(Q_one, Q_two)/c)    mid in [0.10, 0.90], c = 3
             = min(Q_one, Q_two)                               otherwise (two-sided only)
    per minute (random second): Q_normal = Q_min / Σ_makers Q_min; per UTC day: Σ over 1440
    minutes, normalised again; payout = share × the market's rate_per_day, at midnight UTC, ≥ 1 USD.

Fees and maker rebates (docs …/trading/fees, …/market-makers/maker-rebates):

    taker fee = C · f · p(1 − p);  makers never pay
    rebate    = r · (maker's fee equivalent C · f · p(1 − p))   (pool = r × the market's taker fees,
                                                               split by fee equivalent: exact per share)
"""
from __future__ import annotations

import numpy as np

DOCS_DATE = "2026-11-10"
C = 3.0
TWO_SIDED_BAND = (0.10, 0.90)

# category → (taker fee rate f, maker rebate share r), docs as of DOCS_DATE
FEES = {
    "Crypto": (0.07, 0.20), "Sports": (0.05, 0.15), "Finance": (0.04, 0.25), "Politics": (0.04, 0.25),
    "Economics": (0.05, 0.25), "Culture": (0.05, 0.25), "Weather": (0.05, 0.25), "Other": (0.05, 0.25),
    "Mentions": (0.04, 0.25), "Tech": (0.04, 0.25), "Geopolitics": (0.0, 0.0),
}


def score_weight(v_c: np.ndarray | float, s_c: np.ndarray | float) -> np.ndarray:
    """S(v, s) = ((v − s)/v)² inside the max spread, 0 outside. Vectorised over arrays."""
    v = np.asarray(v_c, dtype=float)
    s = np.abs(np.asarray(s_c, dtype=float))
    with np.errstate(divide="ignore", invalid="ignore"):
        w = np.where((v > 0) & (s < v), ((v - s) / v) ** 2, 0.0)
    return w


def adjusted_mid(bid_px: np.ndarray, bid_sz: np.ndarray, ask_px: np.ndarray, ask_sz: np.ndarray, min_size: float) -> float | None:
    """Midpoint of the best bid and ask among levels holding at least `min_size` shares.
    Why at level size: L2 cannot see single orders, so the size cutoff is applied to the level
    (an upper bound on what qualifies; documented limitation)."""
    b = bid_px[bid_sz >= min_size]
    a = ask_px[ask_sz >= min_size]
    if not len(b) or not len(a):
        return None
    bb, ba = b.max(), a.min()
    if ba <= bb:
        return None
    return float((bb + ba) / 2)


def side_scores(bid_px, bid_sz, ask_px, ask_sz, mid: float, v_c: float, min_size: float) -> tuple[float, float]:
    """(Q_one, Q_two) of all qualifying resting size: levels of at least min_size within v of mid."""
    qb = bid_sz >= min_size
    qa = ask_sz >= min_size
    one = float((score_weight(v_c, (mid - bid_px[qb]) * 100) * bid_sz[qb]).sum())
    two = float((score_weight(v_c, (ask_px[qa] - mid) * 100) * ask_sz[qa]).sum())
    return one, two


def q_min(q_one: float, q_two: float, mid: float, c: float = C) -> float:
    lo, hi = TWO_SIDED_BAND
    if lo <= mid <= hi:
        return max(min(q_one, q_two), max(q_one, q_two) / c)
    return min(q_one, q_two)


def competing_bracket(q_one: float, q_two: float, mid: float, c: float = C) -> tuple[float, float, float]:
    """Σ over makers of Q_min, from the side totals only (L2 hides who owns what): (low, central, high).

    In the two-sided band, for one maker with sides a ≥ b, Q_min = max(b, a/c), and
        max(b, a/c) ≤ b + (a − b)/c          (equality at b = a or b = 0)
        max(b, a/c) ≥ a/c  and  ≥ (a + b)/(1 + c)
    Summing over makers with m = min(Q_one, Q_two), x = max(…), T = m + x, and Σ min_n ≤ m:
        high = m + (x − m)/c          (the tightest upper bound: all makers either balanced or one-sided)
        low  = max(T/(1 + c), x/c)
    Outside the band Q_min = min(a, b): high = m, low = 0.
    central = the midpoint of the two. Why not one of the ends: the split between makers is unknown,
    and a point between the bounds keeps the error at most half the bracket either way.
    (v1 used T/2 as the high end and m + (x − m)/c as central: audit 2026-11-10, finding 1; on the
    lopsided Fed books T/2 overstated the competition by ~32 %.)"""
    lo_b, hi_b = TWO_SIDED_BAND
    t = q_one + q_two
    m, x = min(q_one, q_two), max(q_one, q_two)
    if lo_b <= mid <= hi_b:
        lo, hi = max(t / (1 + c), x / c), m + (x - m) / c
    else:
        lo, hi = 0.0, m
    return lo, (lo + hi) / 2, hi


def taker_fee_ps(p, f):
    return f * p * (1 - p)


def rebate_ps(p, f, r):
    return r * f * p * (1 - p)
