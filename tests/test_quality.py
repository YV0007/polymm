"""Quality checks on small synthetic inputs: every check must flag the defect it
exists for. Frames are registered as DuckDB views under the table names the
checks read, so the SQL under test is exactly the production SQL."""
from __future__ import annotations

import math

import duckdb
import polars as pl
import pytest

from polymm.quality import (
    NS,
    THRESHOLDS,
    check_book_sanity,
    check_coverage,
    check_cross_feed,
    check_latency,
    check_mirror,
    check_spot,
    check_trades,
    define_window_views,
    register_frames,
    strict_windows,
    xcorr_1s,
)

T0 = 1_794_000_000 * NS  # some instant in 2026, whole second
YES, NO = "yes-token", "no-token"


# ---- builders ------------------------------------------------------------------------


def assets_frame() -> pl.DataFrame:
    return pl.DataFrame({
        "asset_id": [YES, NO], "condition_id": ["c", "c"], "family": ["test", "test"],
        "outcome_index": pl.Series([0, 1], dtype=pl.Int32), "sibling": [NO, YES],
        "market_slug": ["m", "m"], "end_date": ["", ""],
    })


def tob_frame(rows: list[tuple]) -> pl.DataFrame:
    """rows: (ts, asset, best_bid, bid_size, best_ask, ask_size, valid)."""
    return pl.DataFrame(rows, schema={"ts_recv_ns": pl.Int64, "asset_id": pl.String, "best_bid": pl.Float64,
                                      "bid_size": pl.Float64, "best_ask": pl.Float64, "ask_size": pl.Float64,
                                      "valid": pl.Boolean}, orient="row") \
        .with_columns(pl.lit("delta").alias("event_type"), (pl.col("best_ask") - pl.col("best_bid")).alias("spread"))


def windows_frame(rows: list[tuple]) -> pl.DataFrame:
    """rows: (asset, t_start, t_end, last_verified | None)."""
    return pl.DataFrame(rows, schema={"asset_id": pl.String, "t_start": pl.Int64, "t_end": pl.Int64,
                                      "last_verified": pl.Int64}, orient="row") \
        .with_columns(pl.lit("snapshot_ws").alias("start_kind"), pl.lit("end").alias("end_reason"), pl.lit(1).cast(pl.Int64).alias("n_events"))


def events_frame(rows: list[tuple]) -> pl.DataFrame:
    """book_events deltas: (ts, asset, side, price, size); ts_exch_ms = ts in ms minus `lat_ms` if given as 6th."""
    out = []
    for r in rows:
        ts, aid, side, price, size = r[:5]
        lat = r[5] if len(r) > 5 else 10.0
        out.append((ts, int(ts / 1e6 - lat), aid, "delta", None, side, price, size))
    return pl.DataFrame(out, schema={"ts_recv_ns": pl.Int64, "ts_exch_ms": pl.Int64, "asset_id": pl.String, "event_type": pl.String,
                                     "snapshot_id": pl.Int64, "side": pl.String, "price": pl.Float64, "size": pl.Float64}, orient="row")


def labeled_frame(rows: list[tuple]) -> pl.DataFrame:
    """events_labeled: (ts, asset, side, price, size_before, size_after, label, trade_ts, trade_size)."""
    return pl.DataFrame(rows, schema={"ts_recv_ns": pl.Int64, "asset_id": pl.String, "side": pl.String, "price": pl.Float64,
                                      "size_before": pl.Float64, "size_after": pl.Float64, "label": pl.String,
                                      "trade_ts_recv_ns": pl.Int64, "trade_size": pl.Float64}, orient="row") \
        .with_columns((pl.col("size_after") - pl.col("size_before")).alias("size_change"),
                      (pl.col("ts_recv_ns") - pl.col("trade_ts_recv_ns")).alias("trade_lag_ns"))


def trades_frame(rows: list[tuple]) -> pl.DataFrame:
    """(ts, asset, price, size, side)"""
    return pl.DataFrame(rows, schema={"ts_recv_ns": pl.Int64, "asset_id": pl.String, "price": pl.Float64, "size": pl.Float64,
                                      "side": pl.String}, orient="row").with_columns((pl.col("ts_recv_ns") // 1_000_000 - 10).alias("ts_exch_ms"))


def world(con: duckdb.DuckDBPyConnection, t1: int = T0 + 100 * NS, **frames: pl.DataFrame) -> None:
    register_frames(con, assets=assets_frame(), **frames)
    if "clean_windows" in frames and "tob" in frames:
        define_window_views(con, T0, t1)


def find(res, name: str, family: str = "all"):
    for c in res.checks:
        if c.name == name and c.family == family:
            return c
    raise AssertionError(f"check {name!r} [{family}] not produced; have {[c.name for c in res.checks]}")


# ---- coverage ------------------------------------------------------------------------


def test_coverage_strict_uses_last_verified_and_clips_to_span():
    con = duckdb.connect()
    tob = tob_frame([(T0, YES, 0.5, 1, 0.52, 1, True), (T0 + 80 * NS, YES, 0.5, 1, 0.52, 1, True)])
    # window claims to run to T0+100 s but the token's last event is at +80 s; verified at +40 s
    cw = windows_frame([(YES, T0, T0 + 100 * NS, T0 + 40 * NS)])
    world(con, tob=tob, clean_windows=cw)
    res = check_coverage(con, T0, T0 + 100 * NS)
    assert find(res, "clean share (loose)").value == pytest.approx(1.0)
    assert find(res, "clean share (strict)").value == pytest.approx(0.5)
    sw = strict_windows(con)
    assert sw.height == 1 and sw["t_end"][0] == T0 + 40 * NS and sw["asset_strict_share"][0] == pytest.approx(0.5)


def test_sibling_verification_is_inherited():
    con = duckdb.connect()
    tob = tob_frame([(T0, YES, 0.5, 1, 0.52, 1, True), (T0 + 80 * NS, YES, 0.5, 1, 0.52, 1, True),
                     (T0, NO, 0.48, 1, 0.5, 1, True), (T0 + 80 * NS, NO, 0.48, 1, 0.5, 1, True)])
    cw = windows_frame([(YES, T0, T0 + 80 * NS, T0 + 60 * NS), (NO, T0, T0 + 80 * NS, None)])
    world(con, tob=tob, clean_windows=cw)
    sw = strict_windows(con)
    assert set(sw["asset_id"]) == {YES, NO}
    assert sw.filter(pl.col("asset_id") == NO)["t_end"][0] == T0 + 60 * NS
    assert not sw.filter(pl.col("asset_id") == NO)["verified_own"][0]


# ---- book sanity -----------------------------------------------------------------------


def test_crossed_book_detected():
    con = duckdb.connect()
    tob = tob_frame([(T0, YES, 0.50, 1, 0.52, 1, True),
                     (T0 + 10 * NS, YES, 0.53, 1, 0.52, 1, False),  # crossed for 5 s
                     (T0 + 15 * NS, YES, 0.50, 1, 0.52, 1, True),
                     (T0 + 90 * NS, YES, 0.50, 1, 0.52, 1, True)])
    cw = windows_frame([(YES, T0, T0 + 100 * NS, T0 + 90 * NS)])
    world(con, tob=tob, clean_windows=cw)
    c = find(check_book_sanity(con), "crossed book (bid >= ask)")
    assert c.n == 1 and c.value == pytest.approx(5.0) and c.status == "PASS"
    strict = dict(THRESHOLDS, crossed_seconds_warn=1.0, crossed_seconds_fail=4.0)
    assert find(check_book_sanity(con, strict), "crossed book (bid >= ask)").status == "FAIL"


def test_crossed_outside_window_is_ignored():
    con = duckdb.connect()
    tob = tob_frame([(T0, YES, 0.50, 1, 0.52, 1, True), (T0 + 50 * NS, YES, 0.60, 1, 0.52, 1, False),
                     (T0 + 90 * NS, YES, 0.50, 1, 0.52, 1, True)])
    cw = windows_frame([(YES, T0, T0 + 40 * NS, T0 + 30 * NS)])  # the crossed row lies after the window
    world(con, tob=tob, clean_windows=cw)
    assert find(check_book_sanity(con), "crossed book (bid >= ask)").n == 0


# ---- mirror --------------------------------------------------------------------------


def test_mirror_mismatch_detected_pre_a1():
    con = duckdb.connect()
    rows = []
    for k in range(3):
        t = T0 + k * NS
        rows.append((t, YES, True, 0.50, 10.0, 0.52, 7.0))
        no_ask = 0.50 if k < 2 else 0.49  # third instant: NO ask != 1 - YES bid
        rows.append((t, NO, True, 0.48, 7.0, no_ask, 10.0))
    depth = pl.DataFrame(rows, schema={"ts_recv_ns": pl.Int64, "asset_id": pl.String, "valid": pl.Boolean, "bid_px_1": pl.Float64,
                                       "bid_cum_1": pl.Float64, "ask_px_1": pl.Float64, "ask_cum_1": pl.Float64}, orient="row")
    world(con, depth_1s=depth)
    c = find(check_mirror(con, T0, T0 + 10 * NS), "pre-A1: YES/NO prices mirrored (1 s grid)")
    assert c.n == 3 and c.value == pytest.approx(2 / 3) and c.status == "FAIL"


def test_mirror_kept_rows_classified_in_a1_era():
    con = duckdb.connect()
    t1, t2 = T0 + 1 * NS, T0 + 2 * NS
    ev = events_frame([
        (t1, YES, "BUY", 0.40, 10.0), (t1, NO, "SELL", 0.60, 10.0),  # exact mirror, kept because of the next row
        (t1, NO, "SELL", 0.70, 5.0),  # no outcome-0 twin
        (t2, YES, "BUY", 0.41, 10.0), (t2, NO, "SELL", 0.59, 7.0),  # size differs
    ])
    health = pl.DataFrame({"ts_ns": [T0, T0 + NS // 2], "mirror_dropped": [0, 3]})
    world(con, book_events=ev, health=health)
    res = check_mirror(con, T0, T0 + 10 * NS)
    kept = res.tables["mirror_kept"]
    by = dict(zip(kept["reason"].to_list(), kept["n"].to_list()))
    assert by == {"exact_mirror_kept": 1, "no_mirror_row": 1, "size_differs": 1}
    c = find(res, "A1: kept outcome-1 deltas / outcome-0 deltas")
    assert c.n == 2 and c.value == pytest.approx(1.5) and c.status == "FAIL"  # 150 % is beyond the fail bound too


# ---- trades ----------------------------------------------------------------------------


def test_trade_outside_spread_detected():
    con = duckdb.connect()
    tob = tob_frame([(T0, YES, 0.50, 10, 0.52, 10, True), (T0 + 90 * NS, YES, 0.50, 10, 0.52, 10, True)])
    cw = windows_frame([(YES, T0, T0 + 100 * NS, T0 + 90 * NS)])
    tr = trades_frame([(T0 + 10 * NS, YES, 0.52, 5.0, "BUY"),  # at the ask: fine
                       (T0 + 20 * NS, YES, 0.50, 5.0, "SELL"),  # at the bid: fine
                       (T0 + 30 * NS, YES, 0.45, 5.0, "BUY")])  # taker BUY below the bid: impossible
    # the first trade's fill delta: ask level 0.52 shrinks 5 ms before the trade message
    el = labeled_frame([(T0 + 10 * NS - 5_000_000, YES, "SELL", 0.52, 10.0, 5.0, "fill", T0 + 10 * NS, 5.0)])
    world(con, tob=tob, clean_windows=cw, trades=tr, events_labeled=el)
    res = check_trades(con)
    assert find(res, "price inside/at the spread (pre-trade book)").value == pytest.approx(2 / 3)
    assert find(res, "side consistent (BUY >= ask, SELL <= bid)").value == pytest.approx(2 / 3)
    w = find(res, "wrong side (BUY < bid or SELL > ask)")
    assert w.value == pytest.approx(1 / 3) and w.status == "FAIL"
    assert find(res, "trade has a size decrease at its price within 1 s").value == pytest.approx(1 / 3)


def test_trade_matched_through_sibling_and_pre_trade_book():
    """A NO trade stored on the NO token is matched to the YES delta at 1-p, and the
    book is read just before that delta (which here moved the ask up)."""
    con = duckdb.connect()
    t_delta = T0 + 10 * NS
    tob = tob_frame([(T0, NO, 0.48, 10, 0.50, 10, True),
                     (t_delta, NO, 0.48, 10, 0.51, 10, True),  # after the fill the 0.50 ask is gone
                     (T0 + 90 * NS, NO, 0.48, 10, 0.51, 10, True),
                     (T0, YES, 0.50, 10, 0.52, 10, True), (T0 + 90 * NS, YES, 0.50, 10, 0.52, 10, True)])
    cw = windows_frame([(YES, T0, T0 + 100 * NS, T0 + 90 * NS), (NO, T0, T0 + 100 * NS, T0 + 90 * NS)])
    tr = trades_frame([(t_delta + 20_000_000, NO, 0.50, 10.0, "BUY")])  # trade message 20 ms after the delta
    el = labeled_frame([(t_delta, YES, "BUY", 0.50, 10.0, 0.0, "fill", t_delta + 20_000_000, 10.0)])
    world(con, tob=tob, clean_windows=cw, trades=tr, events_labeled=el)
    res = check_trades(con)
    assert find(res, "trade has a size decrease at its price within 1 s").value == pytest.approx(1.0)
    assert find(res, "side consistent (BUY >= ask, SELL <= bid)").value == pytest.approx(1.0)


# ---- latency ------------------------------------------------------------------------


def test_latency_drift_detected():
    con = duckdb.connect()
    rows = []
    for k in range(3 * 360):  # one delta every 10 s for 3 h, latency floor rising 5 ms per hour
        t = T0 + k * 10 * NS
        hours = k * 10 / 3600
        rows.append((t, YES, "BUY", 0.5, 1.0, 10.0 + 5.0 * hours + (k % 3)))  # small jitter above the floor
    world(con, book_events=events_frame(rows))
    res = check_latency(con)
    d = find(res, "clock drift (slope of per-minute p5)")
    assert d.value == pytest.approx(5.0, abs=0.3) and d.status == "WARN"
    assert find(res, "delta recv - exch p50", "test").status == "PASS"
    assert find(res, "negative latency (recv before exch)").value == 0


def test_negative_latency_counted():
    con = duckdb.connect()
    rows = [(T0 + k * NS, YES, "BUY", 0.5, 1.0, 10.0) for k in range(99)] + [(T0 + 99 * NS, YES, "BUY", 0.5, 1.0, -3.0)]
    world(con, book_events=events_frame(rows))
    c = find(check_latency(con), "negative latency (recv before exch)")
    assert c.value == pytest.approx(0.01) and c.status == "WARN"


# ---- spot ------------------------------------------------------------------------------


def test_trade_id_hole_and_quote_gap_detected():
    con = duckdb.connect()
    ids = list(range(1, 11)) + [13, 14, 15]  # 11 and 12 missing
    st = pl.DataFrame({"ts_recv_ns": [T0 + i * NS for i in range(len(ids))], "ts_exch_ms": [T0 // 1_000_000 + i * 1000 for i in range(len(ids))],
                       "event_ms": [T0 // 1_000_000 + i * 1000 for i in range(len(ids))], "symbol": ["BTCUSDT"] * len(ids),
                       "trade_id": ids, "price": [100.0] * len(ids), "qty": [1.0] * len(ids)})
    qts = [T0, T0 + NS, T0 + 2 * NS, T0 + 9 * NS, T0 + 10 * NS]  # 7 s gap
    uids = [1, 2, 4, 3, 5]  # one non-monotonic step
    sq = pl.DataFrame({"ts_recv_ns": qts, "symbol": ["BTCUSDT"] * 5, "update_id": uids, "bid": [99.0] * 5, "bid_qty": [1.0] * 5,
                       "ask": [100.0] * 5, "ask_qty": [1.0] * 5})
    world(con, spot_trades=st, spot_quotes=sq)
    res = check_spot(con)
    h = find(res, "trade_id holes")
    assert h.value == 1 and h.status == "WARN" and "2 ids missing" in h.note
    assert find(res, "quote update_id monotonic (violations)").value == 1
    g = find(res, f"quote gaps > {THRESHOLDS['spot_gap_s']:.0f} s")
    assert g.value == 1 and "7.0 s" in g.note


def test_spot_absent_is_skipped():
    con = duckdb.connect()
    world(con)
    assert find(check_spot(con), "Binance feed").status == "SKIP"


# ---- cross-feed ---------------------------------------------------------------------------


def test_xcorr_finds_binance_lead():
    import random
    rng = random.Random(1)
    n, lag = 1200, 2
    steps = [rng.gauss(0, 1) for _ in range(n + lag)]
    price = [100000.0]
    for s in steps:
        price.append(price[-1] * (1 + s * 1e-4))
    secs = list(range(n))
    bz = pl.DataFrame({"sec": secs, "bmid": price[:n]})
    # Polymarket mid follows the Binance move `lag` seconds later, with noise
    poly = pl.DataFrame({"sec": secs, "slug": ["w"] * n, "start_s": [0] * n,
                         "mid": [0.5 + 50 * (math.log(price[max(i - lag, 0)]) - math.log(price[0])) + rng.gauss(0, 0.002) for i in range(n)]})
    xc = xcorr_1s(poly, bz, max_lag=10)
    best = xc.drop_nulls("corr").sort("corr", descending=True).row(0)
    assert best[0] == lag and best[1] > 0.3


def test_cross_feed_skipped_without_spot():
    con = duckdb.connect()
    world(con)
    assert find(check_cross_feed(con), "Binance vs Polymarket lead (1 s xcorr)", "btc_15m").status == "SKIP"
