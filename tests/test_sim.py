"""Stage-7 engine checks on toy books with answers worked out by hand (D-032)."""
from __future__ import annotations

import numpy as np
import polars as pl

from polymm.sim import attrib as A
from polymm.sim import engine as E
from polymm.sim import experiments as X

S = 1_000_000_000
BIG = np.iinfo(np.int64).max


def _run(el, tr, bk, fr, script=None, rung=-1, prm=None, model="matched", lat_ms=0.0, t1=100 * S, t_end=200 * S, q0=0.0, front=False):
    i64, f64 = np.int64, np.float64
    el = el or []
    tr = tr or []
    col = lambda rows, k, dt: np.array([r[k] for r in rows], dtype=dt)
    sc = script or []
    out = E.run_segment(col(el, 0, i64), col(el, 1, i64), col(el, 2, i64), col(el, 3, f64), col(el, 4, f64), col(el, 5, f64),
                        col(el, 6, i64), col(el, 0, i64),
                        col(tr, 0, i64), col(tr, 1, i64), col(tr, 2, i64), col(tr, 3, f64),
                        col(bk, 0, i64), col(bk, 1, i64), col(bk, 2, i64),
                        col(fr, 0, i64), col(fr, 1, f64), np.zeros(len(fr)),
                        i64(0), np.zeros(1), i64(0), i64(t1), i64(t_end), i64(0), float(q0),
                        i64(rung), prm if prm is not None else E.params(), i64(E.MODEL_ID[model]), float(lat_ms), 0.0, i64(1), i64(10),
                        np.zeros((3, 4, 4)), 200.0, np.zeros((3, 4, 4)),
                        col(sc, 0, i64), col(sc, 1, i64), col(sc, 2, i64), col(sc, 3, f64), np.array([r[4] for r in sc], dtype=i64),
                        i64(16), i64(1000), front, -1.0)
    f_q = out[4]
    return {"fill_qty": float(f_q.sum()), "fills": out[0], "through": out[5], "end": out[18], "q": out[20], "orders": out[24], "n_rej": out[23],
            "o_s": out[14], "o_pk": out[15]}


# a bid of 5 at 0.500 joins 10 others; a taker eats 4; 10 join behind; 3 cancel; a taker eats 8
EL = [(0, 1, 500, 0.0, 10.0, 10.0, -1), (2 * S, 1, 500, 4.0, 0.0, 6.0, -1), (int(2.5 * S), 1, 500, 0.0, 10.0, 16.0, -1),
      (3 * S, 1, 500, 0.0, -3.0, 13.0, -1), (4 * S, 1, 500, 8.0, 0.0, 5.0, -1)]
BK = [(0, 500, 520)]
FR = [(0, 0.51)]
ORDER = [(1 * S, 1, 500, 5.0, BIG)]


def test_queue_worked_example_by_cancel_model():
    # x after the first taker: 10 − 4 = 6; the cancel of 3 is ahead by: pess 0, prop 3·6/16, opt 3
    # the second taker's 8 then reaches us beyond x: pess 8 − 6 = 2, prop 8 − 4.875 = 3.125, opt 8 − 3 = 5
    assert abs(_run(EL, [], BK, FR, ORDER, model="pess")["fill_qty"] - 2.0) < 1e-9
    assert abs(_run(EL, [], BK, FR, ORDER, model="prop")["fill_qty"] - 3.125) < 1e-9
    assert abs(_run(EL, [], BK, FR, ORDER, model="opt")["fill_qty"] - 5.0) < 1e-9


def test_matched_cancel_uses_join_time():
    # the cancel matches an add made BEFORE we arrived (addt = 0.5 s) → ahead; after (addt = 2.5 s) → behind
    el_a = [r if r[0] != 3 * S else (3 * S, 1, 500, 0.0, -3.0, 13.0, int(0.5 * S)) for r in EL]
    el_b = [r if r[0] != 3 * S else (3 * S, 1, 500, 0.0, -3.0, 13.0, int(2.5 * S)) for r in EL]
    assert abs(_run(el_a, [], BK, FR, ORDER, model="matched")["fill_qty"] - 5.0) < 1e-9
    assert abs(_run(el_b, [], BK, FR, ORDER, model="matched")["fill_qty"] - 2.0) < 1e-9


def test_trade_through_our_price_fills_us():
    # our ask at 0.530; a taker buys at 0.540 (maker side −1, price worse for the taker than ours)
    el = [(0, -1, 530, 0.0, 50.0, 50.0, -1)]
    out = _run(el, [(5 * S, -1, 540, 7.0)], [(0, 500, 530)], FR, [(1 * S, -1, 530, 5.0, BIG)])
    assert out["fill_qty"] == 5.0 and bool(out["through"][0])


def test_post_only_rejection():
    out = _run([], [], [(0, 500, 520)], FR, [(1 * S, 1, 520, 5.0, BIG)])
    assert out["n_rej"] == 1 and out["fill_qty"] == 0.0 and out["end"][0] == 3


def test_cancel_landing_after_the_fill_is_too_late():
    # cancel effective at 4 s exactly: the taker at 4 s is processed first (market before ours at a tie)
    o_late = [(1 * S, 1, 500, 5.0, 4 * S)]
    o_early = [(1 * S, 1, 500, 5.0, int(3.9 * S))]
    assert _run(EL, [], BK, FR, o_late, model="opt")["fill_qty"] == 5.0
    assert _run(EL, [], BK, FR, o_early, model="opt")["fill_qty"] == 0.0


def test_do_nothing_strategy_sends_nothing():
    out = _run(EL, [], BK, FR, rung=1, prm=E.params(qmax=0.0))
    assert out["orders"] == 0 and out["fill_qty"] == 0.0


def test_symmetric_quote_prices_and_timer():
    # fair 0.51, h = 1 ¢: bid 0.50, ask 0.52; book 0.48 / 0.55 → both rest; rung 0 re-quotes every 2 s only
    bk = [(0, 480, 550)]
    el = [(0, 1, 500, 0.0, 0.0, 0.0, -1)]
    out = _run(el, [], bk, [(0, 0.51)], rung=0, prm=E.params(h=1.0, req_ms=2000.0), t1=9 * S)
    assert out["orders"] == 2  # one bid + one ask at t0; nothing changes, so the timer keeps them


def test_attribution_identity():
    f = pl.DataFrame({"t": [10 * S, 20 * S], "t_pre": [10 * S, 20 * S], "s": [1, -1], "pk": [500, 530], "qty": [5.0, 3.0], "through": [False, True],
                      "x_arr": [0.0, 0.0], "typ": [1, 1], "oid": [0, 1], "inv": [5.0, 2.0], "yes": ["y", "y"], "condition_id": ["c", "c"], "seg": [0, 0]})
    fair = pl.DataFrame({"yes": ["y"] * 3, "t": [0, 15 * S, 70 * S], "p": [0.51, 0.52, 0.60]})
    seg = pl.DataFrame({"yes": ["y"], "t_end_s": [100], "outcome": [1]})
    a = A.attribute(f, fair, seg, 0.014)
    assert A.identity_error(a) < 1e-12
    # bought 5 at 0.50, settles at 1: hold P&L 2.5; sold 3 at 0.53: −1.41
    assert abs(a["pnl_hold"].sum() - (2.5 - 1.41)) < 1e-9


def test_deflated_sharpe_penalises_many_trials():
    rng = np.random.default_rng(0)
    trials = rng.normal(0, 0.05, 200)
    lo = X.deflated_sharpe(0.05, 300, 0.0, 3.0, trials)["dsr"]
    hi = X.deflated_sharpe(0.30, 300, 0.0, 3.0, trials)["dsr"]
    assert lo < 0.5 < hi


def test_walk_forward_never_selects_on_the_test_day():
    W = pl.DataFrame({"date": ["d1", "d1", "d2", "d2", "d3", "d3"], "rung": [1] * 6, "cfg": ["a", "b"] * 3,
                      "pnl": [1.0, 0.0, -5.0, 9.0, 0.0, 0.0], "fills": [1] * 6, "shares": [1.0] * 6, "q_peak": [0.0] * 6,
                      **{c: [0.0] * 6 for c in ("c1_spread", "c2_adverse", "c3_inventory", "c4_rebate", "c4_rewards")}})
    s = X.select(W, ["d1", "d2", "d3"])
    # for d2 only d1 counts → "a"; for d3, d1+d2 → b (0+9) beats a (1−5)
    assert s.filter(pl.col("date") == "d2")["cfg"][0] == "a"
    assert s.filter(pl.col("date") == "d3")["cfg"][0] == "b"
    assert s.filter(pl.col("date") == "d3")["fold"][0] == "lockbox"


def test_rewards_rung_never_joins_a_best_price_through_fair():
    # fair 0.48, book 0.50 / 0.51: joining the 0.50 bid would buy 2 ¢ above fair (audit finding 2);
    # the bid must rest at fair − h = 0.47, the ask may join 0.51 (3 ¢ above fair, within h? no: h = 1 ¢ → 0.49 → clamped
    # above the best bid to 0.51 is still ≥ r + h)
    out = _run([], [], [(0, 500, 510)], [(0, 0.48)], rung=5, prm=E.params(h=1.0), t1=3 * S)
    bids = out["o_pk"][out["o_s"] == 1]
    assert len(bids) and bids.max() <= 470


def test_queue_front_oracle_fills_first():
    # first in line: the first taker's 4 shares reach us at once, the second's 8 complete the 5
    out = _run(EL, [], BK, FR, ORDER, model="pess", front=True)
    assert out["fill_qty"] == 5.0 and out["fills"][0] == 2 * S
