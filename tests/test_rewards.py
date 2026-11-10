"""Reward-rule tests: the docs' worked example, the two-sided rule and its band, the competition
bracket's bounds, and our share inside a book (mid shift when we improve, post-only)."""
from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from polymm.rewards.rules import adjusted_mid, competing_bracket, q_min, rebate_ps, score_weight, side_scores
from polymm.rewards.score import Quote, score_minutes


def test_docs_worked_example():
    # docs: midpoint 0.50, max spread 3¢. Side one: 100 at 1¢, 200 at 2¢, 100 (the NO-book ask,
    # i.e. a YES bid) at 1¢ → 111.11; side two: 100 at 1.5¢, 100 at 2¢, 200 at 0.5¢ → 175.
    bp = np.array([0.49, 0.48, 0.49]); bs = np.array([100.0, 200.0, 100.0])
    ap = np.array([0.515, 0.52, 0.505]); as_ = np.array([100.0, 100.0, 200.0])
    q1, q2 = side_scores(bp, bs, ap, as_, 0.50, 3.0, 1.0)
    assert q1 == pytest.approx(111.111, abs=1e-3)
    assert q2 == pytest.approx(175.0, abs=1e-6)
    assert q_min(q1, q2, 0.50) == pytest.approx(111.111, abs=1e-3)


def test_score_weight_and_band():
    assert float(score_weight(4.5, 1.0)) == pytest.approx((3.5 / 4.5) ** 2)
    assert float(score_weight(4.5, 4.5)) == 0.0 and float(score_weight(4.5, 6.0)) == 0.0
    assert q_min(90.0, 0.0, 0.50) == pytest.approx(30.0)      # one-sided earns a third inside 0.10–0.90
    assert q_min(90.0, 0.0, 0.05) == 0.0                      # and nothing near the extremes


def test_competing_bracket_bounds_hold_for_any_split():
    rng = np.random.default_rng(1)
    for _ in range(200):
        a, b = rng.uniform(0, 100, 5), rng.uniform(0, 100, 5)          # five makers' two sides
        b[rng.uniform(size=5) < 0.4] = 0.0                              # some one-sided
        for mid in (0.5, 0.05):
            true = sum(q_min(x, y, mid) for x, y in zip(a, b))
            lo, ce, hi = competing_bracket(a.sum(), b.sum(), mid)
            assert lo - 1e-9 <= true <= hi + 1e-9 and lo <= ce <= hi
    # the bounds are tight: balanced makers reach the high end; a maker whose small side is a third
    # of its big side reaches the low end
    assert competing_bracket(100.0, 100.0, 0.5)[2] == pytest.approx(q_min(100.0, 100.0, 0.5))
    assert competing_bracket(90.0, 30.0, 0.5)[0] == pytest.approx(q_min(90.0, 30.0, 0.5))


def test_adjusted_mid_ignores_small_levels():
    bp = np.array([0.50, 0.49]); bs = np.array([10.0, 500.0])
    ap = np.array([0.52]); as_ = np.array([500.0])
    assert adjusted_mid(bp, bs, ap, as_, 50.0) == pytest.approx(0.505)   # the 10-share bid at 0.50 is ignored


def test_rebate_per_share():
    assert rebate_ps(0.5, 0.07, 0.20) == pytest.approx(0.0035)


def _book(rows):
    return pl.DataFrame([{"yes": "Y", "t": 0, "side": s, "px": p, "sz": z} for s, p, z in rows])


def test_our_share_and_mid_shift():
    # book: bid 0.48 ×1000, ask 0.52 ×1000 (spread 4¢, mid 0.50), v = 4.5¢, min size 50
    B = _book([(1, 0.48, 1000.0), (-1, 0.52, 1000.0)])
    S = score_minutes(B, {"Y": (4.5, 50.0)}, [Quote(0, 0, 1000), Quote(1, 1, 1000)], {"Y": 0.01}).sort("cand")
    join, improve = S.row(0, named=True), S.row(1, named=True)
    # joining the best: same distance and size as the book's (balanced) maker; if that maker is
    # two-sided (the high-competition end of the bracket) we get exactly half
    assert join["share_high_comp"] == pytest.approx(0.5, abs=1e-9)
    assert join["share_high_comp"] <= join["share_central"] <= join["share_low_comp"]
    # quoting 1¢ from the mid improves both sides (0.49 / 0.51): the mid stays 0.50, we score more
    assert improve["bid_px"] == pytest.approx(0.49) and improve["ask_px"] == pytest.approx(0.51)
    assert improve["share_central"] > join["share_central"]
    assert improve["capital"] == pytest.approx(1000 * 0.49 + 1000 * 0.49)
