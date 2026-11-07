"""Shape statistics on synthetic inputs. The pure helpers are tested directly;
the SQL path is tested by registering tiny frames under the table names the
production views read (`depth_1s`, `events_labeled`, `trades`, `sw`, `ctx`), so
the SQL under test is exactly the production SQL."""
from __future__ import annotations

import duckdb
import polars as pl
import pytest

from polymm.shape import (
    NS,
    TICK,
    add_moneyness,
    bucket_sql,
    define_views,
    depth_stats,
    detect_jumps,
    flow_family_from_bucket,
    flow_stats,
    group_sweeps,
    implied_btc,
    mny_bucket,
    register_frames,
    spread_stats,
    tick_bin,
    time_shares,
    trade_stats,
)

T0 = 1_794_000_000  # whole seconds, some instant in 2026
UP, DOWN = "up-token", "down-token"


# ---- pure helpers ---------------------------------------------------------------------------


def test_tick_bin_handles_float_noise_and_sub_tick():
    assert tick_bin(0.53 - 0.52) == "1"  # 0.010000000000000009
    assert tick_bin(0.02) == "2"
    assert tick_bin(0.3 - 0.27) == "3"  # 0.030000000000000027
    assert tick_bin(0.04) == "4-5"
    assert tick_bin(0.05) == "4-5"
    assert tick_bin(0.06) == ">5"
    assert tick_bin(0.001) == "<1"  # 0.001-tick regime near 0 / 1
    assert tick_bin(None) is None


def test_time_shares_weights_by_duration_not_by_event_count():
    # Why: 9 changes in the first second and one state held for 9 s afterwards;
    # a row count would say 90 % "busy", time says 10 %.
    ts = [T0 * NS + i * 100_000_000 for i in range(10)]  # 10 events, 0.1 s apart
    state = ["busy"] * 9 + ["quiet"]
    sh = time_shares(ts, state, T0 * NS + 10 * NS)
    assert sh["busy"] == pytest.approx(0.09)
    assert sh["quiet"] == pytest.approx(0.91)


def test_group_sweeps_groups_by_gap_side_and_token():
    base = T0 * NS
    rows = [
        # one taker BUY order walking two levels, three maker fills 1 ms apart
        (base, UP, "BUY", 0.52, 10.0), (base + 1_000_000, UP, "BUY", 0.52, 5.0), (base + 2_000_000, UP, "BUY", 0.53, 20.0),
        # a SELL in the same ms is a different order
        (base + 1_000_000, UP, "SELL", 0.51, 7.0),
        # 60 ms later: a new BUY order (gap > 50 ms)
        (base + 62_000_000, UP, "BUY", 0.53, 3.0),
        # another token
        (base, DOWN, "BUY", 0.48, 4.0),
    ]
    tr = pl.DataFrame(rows, schema={"ts_recv_ns": pl.Int64, "asset_id": pl.String, "side": pl.String, "price": pl.Float64,
                                    "size": pl.Float64}, orient="row")
    g = group_sweeps(tr, gap_ns=50_000_000)
    assert g.height == 4
    sweep = g.filter((pl.col("asset_id") == UP) & (pl.col("side") == "BUY") & (pl.col("ts") == base)).row(0, named=True)
    assert sweep["n_msgs"] == 3 and sweep["size"] == 35.0 and sweep["n_levels"] == 2 and sweep["depth_ticks"] == 2
    assert sweep["usd"] == pytest.approx(10 * 0.52 + 5 * 0.52 + 20 * 0.53)
    later = g.filter((pl.col("asset_id") == UP) & (pl.col("side") == "BUY") & (pl.col("ts") == base + 62_000_000))
    assert later.height == 1 and later["n_msgs"][0] == 1
    assert g.filter(pl.col("side") == "SELL")["n_msgs"][0] == 1
    assert g.filter(pl.col("asset_id") == DOWN)["n_msgs"][0] == 1


def test_group_sweeps_empty():
    assert group_sweeps(pl.DataFrame(schema={"ts_recv_ns": pl.Int64, "asset_id": pl.String, "side": pl.String,
                                             "price": pl.Float64, "size": pl.Float64})).height == 0


def test_detect_jumps_1s_and_aligned_10s():
    # mid flat at 0.50 for 20 s, then jumps to 0.55 at sec 20 (5 ticks), creeps +1 tick at 25, hole at 30
    secs = list(range(0, 30)) + list(range(31, 40))
    mids = [0.50] * 20 + [0.55] * 5 + [0.56] * 5 + [0.56] * 9
    g = pl.DataFrame({"asset_id": [UP] * len(secs), "win": [1] * len(secs), "sec": secs, "mid": mids})
    j1 = detect_jumps(g, 1)
    assert j1.height == len(secs) - 2  # one pair per consecutive second; the hole at 30 drops one pair
    big = j1.filter(pl.col("ticks") >= 2)
    assert big.height == 1 and big["sec"][0] == 20 and big["ticks"][0] == 5
    assert j1.filter(pl.col("sec") == 25)["ticks"][0] == 1
    assert j1.filter(pl.col("sec") == 31).height == 0  # spans the hole
    j10 = detect_jumps(g, 10)
    # aligned blocks only: secs 10, 20 (vs 10: 5 ticks), 30 is missing, so 2 rows; the 5-tick jump is counted once
    assert j10["sec"].to_list() == [10, 20]
    assert j10.filter(pl.col("ticks") >= 5).height == 1
    # a second window of the same asset never pairs with the first
    g2 = pl.concat([g, g.with_columns(pl.lit(2, dtype=pl.Int64).alias("win"), pl.col("sec") + 100)])
    assert detect_jumps(g2, 1).height == 2 * (len(secs) - 2)


def test_implied_btc_interpolates_and_prefers_far_expiry():
    minute = 100
    near, far = minute * 60 + 1800, minute * 60 + 36_000  # 0.5 h and 10 h to expiry
    lad = pl.DataFrame({
        "minute": [minute] * 6, "end_s": [near] * 3 + [far] * 3,
        "strike": [70_000.0, 72_000.0, 74_000.0] * 2,
        "mid": [0.99, 0.99, 0.01,  # near expiry: step function, crossing between 72k and 74k
                0.80, 0.60, 0.20],  # far expiry: 0.5 crossing at 72k + (0.1/0.4)*2000 = 72500
    })
    out = implied_btc(lad, min_hours=1.0)
    assert out.height == 1 and out["btc"][0] == pytest.approx(72_500.0)
    assert implied_btc(lad.filter(pl.col("end_s") == near))["btc"][0] == pytest.approx(72_000 + 0.49 / 0.98 * 2000)
    assert implied_btc(lad.head(0)).height == 0


def test_mny_bucket():
    assert mny_bucket(0.004) == "<0.5%" and mny_bucket(-0.004) == "<0.5%"
    assert mny_bucket(0.015) == "1-2%" and mny_bucket(0.2) == ">5%" and mny_bucket(None) == "n/a"


# ---- SQL path on tiny frames ------------------------------------------------------------------


def depth_rows(sec: int, asset: str, bid: float, ask: float, qb: float = 100.0, qa: float = 100.0, valid: bool = True) -> dict:
    """One depth_1s row with a 5-level book at 1-tick spacing below the bid / above the ask."""
    r = {"ts_recv_ns": sec * NS, "asset_id": asset, "valid": valid, "n_bid_levels": 5 if valid else 0, "n_ask_levels": 5}
    for j in range(1, 6):
        r[f"bid_px_{j}"] = round(bid - (j - 1) * TICK, 3) if valid else None
        r[f"bid_cum_{j}"] = qb * j if valid else None
        r[f"ask_px_{j}"] = round(ask + (j - 1) * TICK, 3)
        r[f"ask_cum_{j}"] = qa * j
    return r


def make_con(depth: list[dict], events: list[tuple] = (), trades: list[tuple] = (), start_s: int | None = None,
             family: str = "btc_15m") -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    d = pl.DataFrame(depth)
    ev = pl.DataFrame(list(events) or None,
                      schema={"ts_recv_ns": pl.Int64, "asset_id": pl.String, "side": pl.String, "price": pl.Float64,
                              "size_before": pl.Float64, "size_after": pl.Float64, "size_change": pl.Float64, "label": pl.String},
                      orient="row")
    tr = pl.DataFrame(list(trades) or None,
                      schema={"ts_recv_ns": pl.Int64, "asset_id": pl.String, "price": pl.Float64, "size": pl.Float64, "side": pl.String},
                      orient="row")
    sw = pl.DataFrame({"asset_id": [UP, DOWN], "t_start": [T0 * NS] * 2, "t_end": [(T0 + 3600) * NS] * 2})
    ctx = pl.DataFrame({"asset_id": [UP, DOWN], "condition_id": ["c", "c"], "family": [family] * 2,
                        "outcome_index": pl.Series([0, 1], dtype=pl.Int32), "sibling": [DOWN, UP], "market_slug": ["m", "m"],
                        "strike": pl.Series([None, None], dtype=pl.Float64), "start_s": pl.Series([start_s, start_s], dtype=pl.Int64),
                        "end_s": pl.Series([None, None], dtype=pl.Int64)})
    register_frames(con, depth_1s=d, events_labeled=ev, trades=tr, sw=sw, ctx=ctx)
    define_views(con)
    add_moneyness(con)
    return con


def test_spread_shares_are_time_weighted_on_the_grid():
    # 9 seconds at 1 tick, 1 second at 3 ticks, outcome-1 token mirrors (must not be double counted),
    # plus one second with an empty bid side (invalid, counts only in the empty-side share)
    depth = [depth_rows(T0 + i, UP, 0.50, 0.51) for i in range(9)] + [depth_rows(T0 + 9, UP, 0.49, 0.52)]
    depth += [depth_rows(T0 + i, DOWN, 0.49, 0.50) for i in range(10)]
    depth += [depth_rows(T0 + 10, UP, 0.50, 0.51, valid=False)]
    con = make_con(depth)
    s = spread_stats(con)
    assert s.height == 1
    r = s.row(0, named=True)
    assert r["n_s"] == 10 and r["n_all"] == 11 and r["n_tokens"] == 1
    assert r["sh_1"] == pytest.approx(0.9) and r["sh_3"] == pytest.approx(0.1)
    assert r["p50_ticks"] == 1 and r["p90_ticks"] == pytest.approx(1.0, abs=0.3)
    assert r["sh_empty_side"] == pytest.approx(1 / 11, abs=1e-4)  # shares are rounded to 4 places


def test_bucket_sql_minute_within_15m_window():
    start = T0 + 60
    depth = [depth_rows(T0 + i, UP, 0.50, 0.51) for i in range(0, 60 + 15 * 60 + 5, 30)]
    con = make_con(depth, start_s=start)
    b = spread_stats(con, "bucket")
    buckets = dict(zip(b["bucket"].to_list(), b["n_s"].to_list()))
    assert buckets["pre"] == 2  # T0, T0+30
    assert buckets["m00"] == 2 and buckets["m14"] == 2
    assert buckets["post"] == 1
    assert con.sql(f"select {bucket_sql('1000')} from (select 'btc_daily_ladder' as family, null as start_s, 1000 + 5 * 3600 as end_s)").fetchall()[0][0] == "h3-6"
    assert con.sql(f"select {bucket_sql('1000')} from (select 'fed_dec_2026' as family, null as start_s, null as end_s)").fetchall()[0][0] == "flat"


def test_depth_within_k_ticks_counts_only_levels_inside_the_band():
    # book: bid 0.50 (100 per level, 5 levels), ask 0.53 (3-tick spread) -> mid 0.515
    # within 1 tick of mid (>= 0.505): nothing, the bid itself is 1.5 ticks away; within 2 (>= 0.495): level 1
    # within 3 (>= 0.485): levels 1-2; within 5 (>= 0.465): levels 1-4
    con = make_con([depth_rows(T0, UP, 0.50, 0.53)])
    d = depth_stats(con).filter(pl.col("side") == "bid")
    got = dict(zip(d["k"].to_list(), d["sh_p50"].to_list()))
    assert got == {1: 0, 2: 100, 3: 200, 5: 400}
    usd = dict(zip(d["k"].to_list(), d["usd_p50"].to_list()))
    assert usd[3] == pytest.approx(100 * 0.50 + 100 * 0.49, abs=0.01)


def test_flow_and_trade_rates_per_market_minute():
    base = T0 * NS
    depth = [depth_rows(T0 + i, UP, 0.50, 0.51) for i in range(120)]  # 2 market-minutes of clean time
    events = [(base + i * NS, UP, "BUY", 0.50, 10.0, 20.0, 10.0, "add") for i in range(6)] + \
             [(base + 30 * NS, UP, "BUY", 0.50, 20.0, 10.0, -10.0, "cancel")] + \
             [(base + 40 * NS, DOWN, "SELL", 0.50, 20.0, 30.0, 10.0, "add")]  # mirror token: not counted
    trades = [(base + 10 * NS, UP, 0.51, 5.0, "BUY"), (base + 10 * NS + 1_000_000, UP, 0.51, 5.0, "BUY"),  # one order, 2 msgs
              (base + 50 * NS, DOWN, 0.49, 8.0, "BUY")]  # taker bought Down = sold Up
    con = make_con(depth, events, trades)
    f = flow_family_from_bucket(flow_stats(con)).row(0, named=True)
    assert f["market_min"] == 2.0 and f["n_add"] == 6 and f["n_cancel"] == 1
    assert f["adds_per_min"] == 3.0 and f["cancels_per_min"] == 0.5 and f["add_sh_per_min"] == 30.0
    fam, byb, orders = trade_stats(con)
    r = fam.row(0, named=True)
    assert r["n_msgs"] == 3 and r["n_orders"] == 2 and r["orders_per_min"] == 1.0
    assert r["yes_buy_sh_count"] == pytest.approx(2 / 3, abs=1e-4) and r["yes_buy_sh_volume"] == pytest.approx(10 / 18, abs=1e-4)
    assert r["sh_multi_msg"] == 0.5 and r["sh_multi_level"] == 0.0
