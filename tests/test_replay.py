"""Replay engine tests: synthetic event sequences with known outcomes, plus one
small slice of real data (skipped when the dataset is not on this machine).

Synthetic data is written as real Parquet files in the recorder's layout, so the
file selection and ordering code is exercised too, not only the book logic."""
from __future__ import annotations

import os
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymm.records import BOOK_EVENTS, MARKETS, TRADES
from polymm.replay import (
    NS,
    MarketIndex,
    ReplayConfig,
    Replayer,
    book_at,
    iter_books,
    parse_ts,
    validation_report,
)

T0 = parse_ts("2026-11-06T15:00:00")
YES, NO = "yes-token", "no-token"
COND = "0xcond"
DATA = Path(os.environ.get("POLYMM_DATA", "/Users/macbook/Desktop/POLY MM/data"))


# ---- helpers -------------------------------------------------------------------------


def _ms(ts_ns: int) -> int:
    return ts_ns // 1_000_000


def snap(ts, aid, bids, asks, hash_="", kind="snapshot_ws", conn=1):
    rows = []
    for p, s in bids:
        rows.append((ts, _ms(ts), aid, COND, kind, ts, "BUY", p, s, hash_, None, None, 0.01, conn))
    for p, s in asks:
        rows.append((ts, _ms(ts), aid, COND, kind, ts, "SELL", p, s, hash_, None, None, 0.01, conn))
    if not rows:
        rows.append((ts, _ms(ts), aid, COND, kind, ts, None, None, None, hash_, None, None, 0.01, conn))
    return rows


def delta(ts, aid, side, price, size, hash_="", conn=1):
    return [(ts, _ms(ts), aid, COND, "delta", None, side, price, size, hash_, None, None, None, conn)]


def reconnect(ts, conn=1):
    return [(ts, 0, "", "", "reconnect", None, None, None, None, "", None, None, None, conn)]


def trade(ts, aid, price, size, side):
    return (ts, _ms(ts), aid, COND, price, size, side, 0, "0xtx", 1)


def write_dataset(root: Path, events: list[tuple], trades: list[tuple] = ()) -> Path:
    """Recorder layout: events/<table>/dt=YYYY-MM-DD/hour=HH/part-*.parquet, one
    file per call so the order inside a file is the order given."""
    def _write(table, schema, rows, layer="events"):
        d = root / layer / table / "dt=2026-11-06" / "hour=15"
        d.mkdir(parents=True, exist_ok=True)
        cols = list(zip(*rows))
        tbl = pa.Table.from_arrays([pa.array(c, type=f.type) for c, f in zip(cols, schema)], schema=schema)
        pq.write_table(tbl, d / "part-150000-00001.parquet")

    _write("book_events", BOOK_EVENTS, events)
    if trades:
        _write("trades", TRADES, list(trades))
    mrows = [
        (T0, "subscribe", "test", "ev", "mk", "q?", COND, YES, "Yes", "", "", 0.01, 5.0, False, False, 0.0, 0.0, 0.0, 0.0, 0.0, True, 0),
        (T0, "subscribe", "test", "ev", "mk", "q?", COND, NO, "No", "", "", 0.01, 5.0, False, False, 0.0, 0.0, 0.0, 0.0, 0.0, True, 1),
    ]
    _write("markets", MARKETS, mrows, layer="reference")
    return root


def run(root: Path, t0=T0, t1=T0 + 600 * NS, **kw) -> tuple[Replayer, dict]:
    out = root / "out"
    cfg = ReplayConfig(data_dir=root, out_dir=out, t0=t0, t1=t1, chunk_s=kw.pop("chunk_s", 60), progress=False, **kw)
    rp = Replayer(cfg)
    stats = rp.run()
    return rp, stats


def derived(root: Path, table: str, asset: str | None = None) -> pl.DataFrame:
    """All rows of a derived table; `asset` filters. Note: a frame with only
    outcome-0 rows is mirrored onto the sibling (A1 rule), so single-token
    scenarios also produce rows for the (uninitialised) NO token."""
    files = sorted((root / "out" / "derived" / table).rglob("*.parquet"))
    assert files, f"no {table} written"
    df = pl.concat([pl.read_parquet(f) for f in files])
    return df.filter(pl.col("asset_id") == asset) if asset else df


# ---- tests: book reconstruction ---------------------------------------------------------


def test_snapshot_deltas_removal(tmp_path):
    ev = snap(T0, YES, [(0.50, 10), (0.49, 20)], [(0.52, 5), (0.53, 7)], "h0")
    ev += delta(T0 + 1 * NS, YES, "BUY", 0.51, 3, "h1")  # new best bid
    ev += delta(T0 + 2 * NS, YES, "SELL", 0.52, 0, "h2")  # best ask removed
    ev += delta(T0 + 3 * NS, YES, "BUY", 0.51, 8, "h3")  # size change, total not increment
    write_dataset(tmp_path, ev)
    rp, _ = run(tmp_path)
    b = rp.assets[YES].book
    assert b.bids == {0.50: 10, 0.49: 20, 0.51: 8}
    assert b.asks == {0.53: 7}
    tob = derived(tmp_path, "tob", YES).sort("ts_recv_ns")
    assert tob["best_bid"].to_list() == [0.50, 0.51, 0.51, 0.51]
    assert tob["best_ask"].to_list() == [0.52, 0.52, 0.53, 0.53]
    assert tob["bid_size"].to_list() == [10, 3, 3, 8]
    assert tob["valid"].all()
    last = tob.row(-1, named=True)
    assert last["mid"] == pytest.approx(0.52) and last["spread"] == pytest.approx(0.02)
    assert last["microprice"] == pytest.approx((0.53 * 8 + 0.51 * 7) / 15)
    # depth grid: as-of semantics (events with ts <= second included), cumulative sizes
    dep = derived(tmp_path, "depth_1s", YES).sort("ts_recv_ns")
    r = dep.filter(pl.col("ts_recv_ns") == T0 + 1 * NS).row(0, named=True)
    assert (r["bid_px_1"], r["bid_px_2"], r["bid_px_3"]) == (0.51, 0.50, 0.49)
    assert (r["bid_cum_1"], r["bid_cum_2"], r["bid_cum_3"]) == (3, 13, 33)
    assert r["bid_px_4"] is None and r["n_bid_levels"] == 3
    r2 = dep.filter(pl.col("ts_recv_ns") == T0 + 2 * NS).row(0, named=True)
    assert r2["ask_px_1"] == 0.53 and r2["n_ask_levels"] == 1
    # one clean window, from the snapshot to the end of the run
    win = derived(tmp_path, "clean_windows", YES)
    assert win.height == 1
    assert win.row(0, named=True)["t_start"] == T0 and win.row(0, named=True)["end_reason"] == "end"


def test_empty_book_and_crossed_are_invalid(tmp_path):
    ev = snap(T0, YES, [(0.50, 10)], [], "h0")  # one-sided book
    ev += delta(T0 + 1 * NS, YES, "SELL", 0.49, 4, "h1")  # ask below bid: crossed
    ev += delta(T0 + 2 * NS, YES, "SELL", 0.49, 0, "h2")
    ev += delta(T0 + 3 * NS, YES, "SELL", 0.55, 4, "h3")  # normal again
    write_dataset(tmp_path, ev)
    run(tmp_path)
    tob = derived(tmp_path, "tob", YES).sort("ts_recv_ns")
    assert tob["valid"].to_list() == [False, False, False, True]
    assert tob["mid"].to_list()[0] is None


def test_reconnect_marks_stale_until_next_snapshot(tmp_path):
    ev = snap(T0, YES, [(0.50, 10)], [(0.52, 5)], "h0")
    ev += delta(T0 + 1 * NS, YES, "BUY", 0.50, 12, "h1")
    ev += reconnect(T0 + 2 * NS)
    ev += delta(T0 + 3 * NS, YES, "BUY", 0.50, 13, "h2", conn=2)  # arrives before the new snapshot
    ev += snap(T0 + 4 * NS, YES, [(0.50, 14)], [(0.52, 5)], "h3", conn=2)
    ev += delta(T0 + 5 * NS, YES, "BUY", 0.50, 15, "h4", conn=2)
    write_dataset(tmp_path, ev)
    rp, stats = run(tmp_path)
    assert stats["reconnects"] == 1
    tob = derived(tmp_path, "tob", YES).sort("ts_recv_ns")
    assert tob["event_type"].to_list() == ["snapshot_ws", "delta", "reconnect", "delta", "snapshot_ws", "delta"]
    assert tob["valid"].to_list() == [True, True, False, False, True, True]
    win = derived(tmp_path, "clean_windows", YES).sort("t_start")
    assert win["t_start"].to_list() == [T0, T0 + 4 * NS]
    assert win["t_end"].to_list()[0] == T0 + 2 * NS and win["end_reason"].to_list() == ["reconnect", "end"]
    dep = derived(tmp_path, "depth_1s", YES).sort("ts_recv_ns")
    assert dep.filter(pl.col("ts_recv_ns") == T0 + 3 * NS)["valid"].item() is False
    assert dep.filter(pl.col("ts_recv_ns") == T0 + 4 * NS)["valid"].item() is True


def test_reconnect_reaches_assets_without_rows_in_chunk(tmp_path):
    # NO has its snapshot in chunk 1 and no rows in chunk 2, where the reconnect falls.
    ev = snap(T0, YES, [(0.50, 10)], [(0.52, 5)], "a")
    ev += snap(T0, NO, [(0.48, 5)], [(0.50, 10)], "b")
    ev += delta(T0 + 70 * NS, YES, "BUY", 0.50, 11, "c")
    ev += reconnect(T0 + 80 * NS)
    ev += snap(T0 + 81 * NS, YES, [(0.50, 11)], [(0.52, 5)], "d", conn=2)
    write_dataset(tmp_path, ev)
    rp, _ = run(tmp_path, chunk_s=60)
    assert rp.assets[NO].stale is True and rp.assets[YES].stale is False
    win = derived(tmp_path, "clean_windows").filter(pl.col("asset_id") == NO)
    assert win["end_reason"].to_list() == ["reconnect"] and win["t_end"].item() == T0 + 80 * NS


# ---- tests: REST validation ----------------------------------------------------------------


def _rest_scenario(tmp_path, rest_offset_ms: int, rest_bids, rest_asks, rest_hash=""):
    ev = snap(T0, YES, [(0.50, 10)], [(0.52, 5)], "h0")
    ev += delta(T0 + 1000 * 10**6, YES, "BUY", 0.50, 11, "h1")  # state S1 at +1000ms
    ev += delta(T0 + 1500 * 10**6, YES, "BUY", 0.50, 12, "h2")  # state S2 at +1500ms
    ev += snap(T0 + (1000 + rest_offset_ms) * 10**6, YES, rest_bids, rest_asks, rest_hash, kind="snapshot_rest", conn=0)
    ev += delta(T0 + 5 * NS, YES, "BUY", 0.50, 13, "h3")
    ev += delta(T0 + 6 * NS, YES, "BUY", 0.50, 14, "h4")
    write_dataset(tmp_path, ev)
    return run(tmp_path)


def test_rest_exact_match(tmp_path):
    rp, _ = _rest_scenario(tmp_path, 1000, [(0.50, 12)], [(0.52, 5)])  # REST at +2000ms == S2
    chk = derived(tmp_path, "rest_checks")
    assert chk["kind"].to_list() == ["exact"] and chk["matched"].all()


def test_rest_behind_matches_earlier_state(tmp_path):
    rp, _ = _rest_scenario(tmp_path, 700, [(0.50, 11)], [(0.52, 5)])  # REST at +1700ms shows S1 (+1000ms)
    chk = derived(tmp_path, "rest_checks")
    assert chk["kind"].to_list() == ["before"] and chk["lag_ns"].item() == -700 * 10**6


def test_rest_ahead_matches_later_state(tmp_path):
    rp, _ = _rest_scenario(tmp_path, 300, [(0.50, 12)], [(0.52, 5)])  # REST at +1300ms already shows S2 (+1500ms)
    chk = derived(tmp_path, "rest_checks")
    assert chk["kind"].to_list() == ["after"] and chk["lag_ns"].item() == 200 * 10**6


def test_rest_hash_counts_as_match(tmp_path):
    # Levels differ (REST carries a level we never saw) but the server hash equals a stream hash.
    rp, _ = _rest_scenario(tmp_path, 100, [(0.50, 11), (0.40, 1)], [(0.52, 5)], rest_hash="h1")
    chk = derived(tmp_path, "rest_checks")
    assert chk["matched"].all() and chk["kind"].item() == "before"


def test_rest_gap_confirmed_resyncs_and_splits_window(tmp_path):
    # REST shows a level (0.45) the stream never delivered: a real gap.
    rp, stats = _rest_scenario(tmp_path, 1000, [(0.50, 12), (0.45, 9)], [(0.52, 5)])
    chk = derived(tmp_path, "rest_checks")
    assert chk["kind"].item() == "gap" and chk["n_diff"].item() == 1
    assert stats["resyncs"] == 1
    # resync = REST book + re-applied deltas since the REST read: the +5s delta arrived
    # after the deadline (+3s) and confirmed the gap; it is re-applied on top of REST.
    b = rp.assets[YES].book
    assert b.bids == {0.50: 14, 0.45: 9} and b.asks == {0.52: 5}
    win = derived(tmp_path, "clean_windows", YES).sort("t_start")
    assert win["end_reason"].to_list() == ["rest_gap", "end"]
    assert win["t_end"].to_list()[0] == T0 + 2 * NS  # old window ends at the REST timestamp
    assert win["t_start"].to_list()[1] == T0 + 5 * NS  # new one starts when the gap was confirmed
    assert win["start_kind"].to_list()[1] == "rest_resync"
    rep = validation_report(rp.rest_checks_all, stats)
    assert "0/1" in rep.replace(" ", "") or "0 matched" in rep or "0.000 %" in rep


def test_rest_before_first_snapshot_is_anchor(tmp_path):
    ev = snap(T0 + 1 * NS, YES, [(0.50, 10)], [(0.52, 5)], "r", kind="snapshot_rest", conn=0)
    ev += delta(T0 + 2 * NS, YES, "BUY", 0.50, 11, "h1")
    write_dataset(tmp_path, ev)
    rp, _ = run(tmp_path)
    chk = derived(tmp_path, "rest_checks")
    assert chk["kind"].item() == "init" and chk["in_window"].item() is False
    assert rp.assets[YES].book.bids == {0.50: 11}
    assert derived(tmp_path, "clean_windows", YES)["start_kind"].item() == "snapshot_rest"


def test_ws_snapshot_mismatch_cuts_window(tmp_path):
    ev = snap(T0, YES, [(0.50, 10)], [(0.52, 5)], "h0")
    ev += delta(T0 + 1 * NS, YES, "BUY", 0.50, 11, "h1")
    ev += snap(T0 + 10 * NS, YES, [(0.50, 11), (0.40, 3)], [(0.52, 5)], "h2")  # level we never saw: deltas were lost
    ev += delta(T0 + 11 * NS, YES, "BUY", 0.50, 12, "h3")
    write_dataset(tmp_path, ev)
    rp, stats = run(tmp_path)
    assert stats["ws_mismatch"] == 1 and rp.ws_mismatches[0][4] == 1
    win = derived(tmp_path, "clean_windows", YES).sort("t_start")
    assert win["end_reason"].to_list() == ["ws_gap", "end"]
    assert win["t_end"].to_list()[0] == T0 + 1 * NS  # ends at the last applied event, not at the snapshot
    assert win["t_start"].to_list()[1] == T0 + 10 * NS and win["start_kind"].to_list()[1] == "snapshot_ws"
    assert rp.assets[YES].book.bids == {0.50: 12, 0.40: 3}


# ---- tests: fill / cancel labelling -----------------------------------------------------------


def test_labels_add_fill_cancel(tmp_path):
    ev = snap(T0, YES, [(0.50, 10)], [(0.52, 5), (0.53, 9)], "h0")
    ev += snap(T0, NO, [(0.48, 5), (0.47, 9)], [(0.50, 10)], "h0n")
    ev += delta(T0 + 1 * NS, YES, "BUY", 0.50, 15, "h1")  # add
    ev += delta(T0 + 2 * NS, YES, "SELL", 0.52, 2, "h2")  # down by 3, trade 0.4 s earlier: fill
    ev += delta(T0 + 3 * NS, YES, "SELL", 0.53, 4, "h3")  # down, no trade: cancel
    ev += delta(T0 + 4 * NS, YES, "BUY", 0.50, 15, "h4")  # unchanged
    ev += delta(T0 + 2 * NS, NO, "BUY", 0.48, 2, "h2n")  # the mirrored fill on the sibling
    ev += delta(T0 + 10 * NS, YES, "BUY", 0.50, 9, "h5")  # down, trade on the wrong side: cancel
    trades = [
        trade(T0 + 1600 * 10**6, YES, 0.52, 3, "BUY"),  # taker bought YES at 0.52: consumes the YES ask
        trade(T0 + 10 * NS, YES, 0.50, 6, "BUY"),  # BUY consumes asks, not the bid at 0.50
    ]
    write_dataset(tmp_path, ev, trades)
    run(tmp_path)
    lab = derived(tmp_path, "events_labeled").sort(["ts_recv_ns", "asset_id"])
    y = lab.filter(pl.col("asset_id") == YES)
    assert y["label"].to_list() == ["add", "fill", "cancel", "unchanged", "cancel"]
    assert y["size_change"].to_list() == [5, -3, -5, 0, -6]
    f = y.filter(pl.col("label") == "fill").row(0, named=True)
    assert f["trade_size"] == 3 and f["trade_lag_ns"] == 400 * 10**6
    # NO: frames without NO rows were mirrored (A1 rule); the +2s frame had its own NO row.
    n = lab.filter(pl.col("asset_id") == NO)
    assert n["mirrored"].to_list() == [True, False, True, True, True]
    f = n.filter(pl.col("ts_recv_ns") == T0 + 2 * NS).row(0, named=True)
    assert f["label"] == "fill" and f["side"] == "BUY" and f["price"] == 0.48 and f["trade_size"] == 3
    assert n.filter(pl.col("ts_recv_ns") == T0 + 3 * NS)["label"].item() == "cancel"  # mirrored cancel at NO bid 0.47


# ---- tests: mirror rule (A1) ------------------------------------------------------------------


def test_outcome0_delta_without_sibling_rows_is_mirrored(tmp_path):
    ev = snap(T0, YES, [(0.50, 10)], [(0.52, 5)], "a")
    ev += snap(T0, NO, [(0.48, 5)], [(0.50, 10)], "b")
    ev += delta(T0 + 1 * NS, YES, "BUY", 0.57, 3, "c")  # frame with only outcome-0 rows -> mirrored
    ev += delta(T0 + 2 * NS, YES, "SELL", 0.52, 6, "d")  # frame with both rows -> not mirrored
    ev += delta(T0 + 2 * NS, NO, "BUY", 0.48, 6, "e")
    write_dataset(tmp_path, ev)
    rp, stats = run(tmp_path)
    assert stats["mirrored_deltas"] == 1
    assert rp.assets[NO].book.asks == {0.50: 10, 0.43: 3}  # 1 - 0.57 rounded, not 0.43000000000000005
    assert rp.assets[NO].book.bids == {0.48: 6}
    lab = derived(tmp_path, "events_labeled").filter(pl.col("asset_id") == NO).sort("ts_recv_ns")
    assert lab["mirrored"].to_list() == [True, False]


# ---- tests: public API --------------------------------------------------------------------------


def test_iter_books_and_book_at(tmp_path):
    ev = snap(T0, YES, [(0.50, 10)], [(0.52, 5)], "h0")
    ev += delta(T0 + 1 * NS, YES, "BUY", 0.50, 11, "h1")
    ev += delta(T0 + 2 * NS, YES, "BUY", 0.50, 12, "h2")
    ev += delta(T0 + 3 * NS, YES, "BUY", 0.50, 13, "h3")
    write_dataset(tmp_path, ev)
    # iter from +2s: the snapshot before t0 is found and applied, only events >= t0 are yielded
    pts = list(iter_books(tmp_path, YES, T0 + 2 * NS, T0 + 10 * NS))
    assert [p.ts_recv_ns for p in pts] == [T0 + 2 * NS, T0 + 3 * NS]
    assert pts[-1].book.bids == {0.50: 13} and pts[-1].valid
    b = book_at(tmp_path, YES, T0 + 2 * NS + 1)
    assert b is not None and b.book.bids == {0.50: 12}
    b = book_at(tmp_path, YES, T0 + 2 * NS)  # as-of includes the event at exactly t
    assert b.book.bids == {0.50: 12}
    assert book_at(tmp_path, YES, T0 - 1) is None


# ---- real data ------------------------------------------------------------------------------------


@pytest.mark.skipif(not (DATA / "events" / "book_events" / "dt=2026-11-06").exists(), reason="dataset not on this machine")
def test_real_slice_matches_rest(tmp_path):
    t0, t1 = parse_ts("2026-11-06T15:00:00"), parse_ts("2026-11-06T15:05:00")
    cfg = ReplayConfig(data_dir=DATA, out_dir=tmp_path / "out", t0=t0, t1=t1, chunk_s=120, progress=False)
    rp = Replayer(cfg)
    stats = rp.run()
    assert stats["rows"] > 100_000
    chk = pl.DataFrame([list(r) for r in rp.rest_checks_all], schema=["ts", "asset_id", "family", "in_window", "matched", "kind", "lag", "nl", "nd", "reason"], orient="row")
    inw = chk.filter(pl.col("in_window"))
    assert inw.height > 20
    assert inw["matched"].mean() >= 0.995
    tob = derived(tmp_path, "tob")
    assert tob.filter(pl.col("valid"))["spread"].min() > 0
