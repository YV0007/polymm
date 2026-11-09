"""Queue-engine tests on hand-built books with known answers: FIFO consumption, the four cancel
models (including the worked example from the stage-5 plan), pseudo-L3, trades through our
price, post-only rejection, being left behind, the x ≤ L cap, and the model ordering
pessimistic ≤ proportional ≤ optimistic on random books."""
from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from polymm.queue.engine import simulate
from polymm.queue.events import _match

NS = 1_000_000_000
P0 = 520  # 0.52 in pk units


def _levels(rows):
    """rows: (t, cons, res, L, addt) at one level: the YES ask at 0.52."""
    return pl.DataFrame([{"yes": "Y", "s": -1, "pk": P0, "level": 0, "t": t, "cons": float(c), "res": float(r), "L": float(L), "addt": a}
                         for t, c, r, L, a in rows], schema_overrides={"s": pl.Int8})


def _book(rows=None):
    rows = rows or [(0, 510, P0)]
    return pl.DataFrame([{"yes": "Y", "t": t, "bid_pk": b, "ask_pk": a} for t, b, a in rows])


def _trades(rows=None):
    rows = rows or []
    return pl.DataFrame([{"yes": "Y", "s": s, "t": t, "pk": pk, "v": float(v)} for t, s, pk, v in rows],
                        schema={"yes": pl.String, "s": pl.Int8, "t": pl.Int64, "pk": pl.Int64, "v": pl.Float64})


def _probe(t_arr=2 * NS, s=-1, pk=P0, t_max=100 * NS):
    return pl.DataFrame({"pid": [0], "yes": ["Y"], "s": [s], "pk": [pk], "t_arr": [t_arr], "t_max": [t_max]},
                        schema_overrides={"s": pl.Int8, "pid": pl.UInt32})


def _run(levels, probe=None, trades=None, book=None):
    return simulate(probe if probe is not None else _probe(), levels, trades if trades is not None else _trades(),
                    book if book is not None else _book()).row(0, named=True)


def test_worked_example_from_the_plan():
    # join behind 300; add 200 behind us; trade 100; cancel 150 (unknown origin); trade 180
    ev = _levels([(1 * NS, 0, 300, 300, -1), (3 * NS, 0, 200, 500, -1), (4 * NS, 100, 0, 400, -1),
                  (5 * NS, 0, -150, 250, -1), (6 * NS, 180, 0, 70, -1)])
    r = _run(ev)
    assert r["x0"] == 300
    assert r["filled5_pess"] == 0 and r["x_end_pess"] == pytest.approx(20)          # 200 → 20 ahead, never reached
    assert r["filled200_prop"] == pytest.approx(55)                                 # x 200 − 150·200/400 = 125; 180 − 125
    assert r["filled200_opt"] == pytest.approx(130)                                 # x 50; 180 − 50
    assert r["t_first_prop"] == 6 * NS and r["t_first_pess"] == -1
    assert r["filled200_matched"] == pytest.approx(55)                              # unmatched → proportional


def test_matched_cancel_ahead_or_behind():
    # 160 ahead of us (two orders: 100 at t=1, 60 at t=1+); 50 joins behind at t=3; the 60-share
    # order that joined BEFORE us cancels (matched): all of it was ahead → x = 100; a 120 trade fills 20.
    ev = [(1 * NS, 0, 100, 100, -1), (1 * NS + 1, 0, 60, 160, -1), (3 * NS, 0, 50, 210, -1),
          (4 * NS, 0, -60, 150, 1 * NS + 1), (5 * NS, 120, 0, 30, -1)]
    r = _run(_levels(ev))
    assert r["filled200_matched"] == pytest.approx(20)
    assert r["filled200_pess"] == 0                            # pessimist: still 160 − 120 = 40 ahead


def test_matched_cancel_behind_us_does_not_help():
    ev = [(1 * NS, 0, 100, 100, -1), (3 * NS, 0, 60, 160, -1), (4 * NS, 0, -60, 100, 3 * NS), (5 * NS, 120, 0, 0, -1)]
    r = _run(_levels(ev))
    assert r["filled200_matched"] == pytest.approx(20)        # the add at t=3 was behind us: x stays 100
    assert r["filled200_prop"] > 20                            # proportional wrongly credits part of it


def test_trade_through_our_price_fills_an_empty_level():
    # improve probe: an ask at 0.51 where nobody rests; a taker buys at 0.52 (worse for them)
    ev = _levels([(1 * NS, 0, 300, 300, -1)])                  # the real level at 0.52
    r = _run(ev, probe=_probe(pk=510), trades=_trades([(3 * NS, -1, 520, 40)]), book=_book([(0, 500, 520)]))
    assert r["x0"] == 0 and r["filled200_pess"] == pytest.approx(40) and r["through_pess"]
    assert r["t_first_pess"] == 3 * NS


def test_post_only_rejection():
    r = _run(_levels([(1 * NS, 0, 300, 300, -1)]), probe=_probe(pk=510), book=_book([(0, 500, 520), (NS, 510, 520)]))
    # an ask at 0.51 with the best bid at 0.51 would cross: rejected
    assert r["stop"] == "rejected" and r["filled5_opt"] == 0


def test_left_behind_stops_the_probe():
    ev = _levels([(1 * NS, 0, 300, 300, -1), (5 * NS, 400, 0, 0, -1)])
    # our ask at 0.52; at t=3 the best ask drops to 0.50 (2 ticks better): a quoter re-prices
    r = _run(ev, book=_book([(0, 510, 520), (3 * NS, 490, 500)]))
    assert r["stop"] == "left_behind" and r["t_stop"] == 3 * NS and r["filled5_opt"] == 0


def test_cap_at_level_size():
    ev = _levels([(1 * NS, 0, 300, 300, -1), (3 * NS, 0, -300, 0, -1), (4 * NS, 0, 50, 50, -1), (5 * NS, 10, 0, 40, -1)])
    r = _run(ev)
    assert r["t_front_pess"] == 3 * NS                         # everyone ahead left: even the pessimist is first
    assert r["filled5_pess"] == pytest.approx(5) and r["filled200_pess"] == pytest.approx(10)


def test_model_ordering_on_random_books():
    rng = np.random.default_rng(3)
    for trial in range(30):
        L, rows, t = 0.0, [], NS
        for _ in range(200):
            t += int(rng.integers(1, 50)) * 10_000_000
            u = rng.random()
            if u < 0.45 or L < 1:
                a = float(rng.integers(1, 200)); L += a; rows.append((t, 0, a, L, -1))
            elif u < 0.9:
                c = float(min(L, rng.integers(1, 200))); L -= c; rows.append((t, 0, -c, L, -1))
            else:
                v = float(min(L, rng.integers(1, 100))); L -= v; rows.append((t, v, 0, L, -1))
        r = _run(_levels(rows), probe=_probe(t_arr=rows[20][0] + 1, t_max=t + NS))
        for q in (5, 50, 200):
            assert r[f"filled{q}_pess"] <= r[f"filled{q}_prop"] + 1e-9 <= r[f"filled{q}_opt"] + 2e-9


def test_pseudo_l3_lifo_match():
    # level 0, size key 100: add t=1, add t=2, cancel t=3 → matches the t=2 add (LIFO); cancel t=4 → t=1
    level = np.array([0, 0, 0, 0]); key = np.array([100, 100, 100, 100])
    kind = np.array([1, 1, -1, -1], np.int8); t = np.array([1, 2, 3, 4]) * NS
    assert _match(level, key, kind, t, 60 * NS).tolist() == [-1, -1, 2 * NS, 1 * NS]
    # too old: an add 61 s earlier is not trusted
    assert _match(level[:2], key[:2], np.array([1, -1], np.int8), np.array([0, 61 * NS]), 60 * NS).tolist() == [-1, -1]


def test_cancel_triggers_alone_and_stale():
    from polymm.queue.analysis import apply_policy
    ev = _levels([(1 * NS, 0, 300, 300, -1), (2 * NS + 5, 0, -300, 0, -1)])
    # our ask at 0.52 joins behind 300; at t=3 the real best ask moves up to 0.53 (everyone at 0.52
    # left): we are alone; at t=6 a taker buys at 0.53 → through us
    book = _book([(0, 510, 520), (3 * NS, 510, 530)])
    fair = pl.DataFrame({"asset_id": "Y", "sec": list(range(0, 10)), "p_fair": [0.515] * 4 + [0.525] * 6})
    R = simulate(_probe(), ev, _trades([(6 * NS, -1, 530, 50)]), book, fair)
    r = R.row(0, named=True)
    assert r["t_alone"] == 3 * NS
    assert r["t_stale"] == 4 * NS                              # fair 0.525 > our ask 0.52 from second 4
    assert r["t_first_pess"] == 6 * NS                          # the passive order is picked off
    assert apply_policy(R, "alone", 128).row(0, named=True)["t_first_pess"] == -1
    assert apply_policy(R, "stale", 5000).row(0, named=True)["t_first_pess"] == 6 * NS   # cancel lands at 8 s: too late
    assert apply_policy(R, "stale", 1000).row(0, named=True)["t_first_pess"] == -1


def test_sweep_already_under_way_does_not_fill_us():
    # improve ask at 0.51 lands at t=2 s; a taker order that STARTED at 1.99 s (its first book
    # decrease) prints through 0.52 at 2.02 s: the exchange matched it before our order existed
    ev = _levels([(1 * NS, 0, 300, 300, -1)])
    tr = _trades([(2 * NS - 10_000_000, -1, 520, 40), (3 * NS, -1, 520, 25)])   # t = order start
    r = _run(ev, probe=_probe(pk=510), trades=tr, book=_book([(0, 500, 520)]))
    assert r["filled200_pess"] == pytest.approx(25) and r["t_first_pess"] == 3 * NS


def test_position_on_arrival():
    ev = _levels([(1 * NS, 0, 300, 300, -1)])
    assert _run(ev)["pos_arr"] == 0                                               # ask at the touch
    assert _run(ev, probe=_probe(pk=510), book=_book([(0, 500, 520)]))["pos_arr"] == -1   # improving
