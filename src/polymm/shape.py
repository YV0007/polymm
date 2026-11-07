"""Stage 2, "shape of the market": stylized facts per market family.

`polymm shape --date D` answers, per family, the questions a market maker asks
before quoting anywhere: is the spread wide or pinned at the tick floor, is the
book deep or thin relative to the trades that hit it, how fast does the book
churn, how big and how clustered are taker orders, and how jumpy is the mid,
all as a function of time to expiry. It writes

    <out>/shape.md            one page of tables, every number with its sample size
    <out>/tables/*.csv        the frames behind the tables (for notebooks / later stages)
    <out>/*.png               one plot per statistic group

Inputs are the replay's derived tables (`depth_1s`, `events_labeled`) plus the raw
`trades` and (VPS days) `spot_quotes`, read ONLY inside the strict clean windows
that `polymm quality` wrote (`clean_windows_strict.parquet`). If those are missing
the quality step (and the replay it needs) is run first.

Why the 1-second grid and not `tob`: `tob` has a row per event, so averaging over
rows over-weights busy minutes. `depth_1s` is an as-of sample of the book at every
whole second, so every second of clean time weighs the same; a share of grid rows
is a time share (to 1 s resolution) and a quantile over grid rows is a
time-weighted quantile. 2.9 M rows for an 11-hour block, versus 42 M `tob` rows.

Why outcome-0 tokens only for book statistics: the YES and NO books are mirror
images (quality report: 100 % mirrored on the grid), so counting both doubles
every sample without adding information. Trades are stored once per fill on the
taker's token, so they are read from both tokens and re-expressed in the YES frame.

Why "per market" rates: an add on the YES book is the same order as an add on the
NO book, so flow is counted on the outcome-0 token and divided by its clean
seconds; "per token" and "per market" are the same number here.
"""
from __future__ import annotations

import logging
import math
import re
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import polars as pl

from .quality import QualityConfig, _files, load_assets, register_frames, run_quality
from .replay import NS

log = logging.getLogger(__name__)

# Why 0.01 everywhere: all three families are 1-cent markets; Polymarket switches
# a market to 0.001 only while its price sits beyond 0.96 / 0.04. One cent is the
# floor a quoter faces in the range where quoting makes sense, so "ticks" means
# cents throughout and a sub-cent spread lands in the "<1" bin.
TICK = 0.01
SPREAD_BINS = ["<1", "1", "2", "3", "4-5", ">5"]
DEPTH_K = (1, 2, 3, 5)  # "within k ticks of mid"
JUMP_TICKS = (2, 5, 10)
JUMP_HORIZONS_S = (1, 10)
SWEEP_GAP_NS = 50_000_000  # trade messages of one token+side closer than this are one taker order
BINANCE_MOVE = 0.0005  # 0.05 % range move in the preceding 10 s counts as "Binance moved"
BINANCE_LOOKBACK_S = 10
CANCEL_HORIZONS_S = (1, 10, 60)
BUCKET_ORDER = (["pre"] + [f"m{i:02d}" for i in range(15)] + ["post"]
                + ["h<1", "h1-3", "h3-6", "h6-12", "h12-24", "h24-48", "h>48"] + ["flat", "n/a"])
MNY_ORDER = ["<0.5%", "0.5-1%", "1-2%", "2-5%", ">5%", "n/a"]
FAMILY_ORDER = ["btc_15m", "btc_daily_ladder", "fed_dec_2026"]


# ---- pure helpers (unit-tested) -----------------------------------------------------------


def tick_bin(spread: float | None, tick: float = TICK) -> str | None:
    """Spread -> one of SPREAD_BINS. Why round to 0.1 tick first: spreads are
    sums of floats (0.53 - 0.52 = 0.010000000000000009) and must not fall into
    the wrong bin because of the last binary digit."""
    if spread is None or not math.isfinite(spread):
        return None
    t = round(spread / tick, 1)
    if t < 1:
        return "<1"
    if t < 2:
        return "1"
    if t < 3:
        return "2"
    if t < 4:
        return "3"
    if t <= 5:
        return "4-5"
    return ">5"


def time_shares(ts: list[int], state: list, t_end: int) -> dict:
    """Share of time spent in each state, from (timestamp, state) step changes
    that hold until the next change and end at `t_end`. The reference semantics
    the 1-second grid approximates; used by the tests to check the SQL path."""
    if not ts:
        return {}
    total = t_end - ts[0]
    out: dict = {}
    for i, (t, s) in enumerate(zip(ts, state)):
        nxt = ts[i + 1] if i + 1 < len(ts) else t_end
        out[s] = out.get(s, 0) + (nxt - t)
    return {k: v / total for k, v in out.items()}


def group_sweeps(tr: pl.DataFrame, gap_ns: int = SWEEP_GAP_NS, tick: float = TICK) -> pl.DataFrame:
    """Group trade messages into taker orders: same asset and taker side, each
    message within `gap_ns` of the previous one. Returns one row per group.

    Why time proximity and not transaction_hash: in this feed every trade message
    has its own hash (quality report §4 item 6), and one taker order that walks
    several levels or several makers prints one message per maker, a few ms
    apart. 50 ms is far above the intra-order spacing (sub-ms to a few ms) and far
    below the typical gap between independent orders.
    Columns: asset_id, side, ts (first message), n_msgs, size, usd, n_levels,
    depth_ticks (= (max price - min price) / tick + 1), px_first, px_last."""
    if tr.height == 0:
        return pl.DataFrame(schema={"asset_id": pl.String, "side": pl.String, "ts": pl.Int64, "n_msgs": pl.UInt32,
                                    "size": pl.Float64, "usd": pl.Float64, "n_levels": pl.UInt32,
                                    "depth_ticks": pl.Int64, "px_first": pl.Float64, "px_last": pl.Float64})
    s = tr.sort(["asset_id", "side", "ts_recv_ns"])
    new = (pl.col("ts_recv_ns") - pl.col("ts_recv_ns").shift(1).over(["asset_id", "side"]) > gap_ns) \
        .fill_null(True).cast(pl.Int64)
    s = s.with_columns(new.cum_sum().over(["asset_id", "side"]).alias("grp"))
    return s.group_by(["asset_id", "side", "grp"], maintain_order=True).agg(
        pl.col("ts_recv_ns").min().alias("ts"), pl.len().alias("n_msgs"), pl.col("size").sum(),
        (pl.col("size") * pl.col("price")).sum().alias("usd"), pl.col("price").n_unique().alias("n_levels"),
        ((pl.col("price").max() - pl.col("price").min()) / tick + 1 + 1e-6).floor().cast(pl.Int64).alias("depth_ticks"),
        pl.col("price").first().alias("px_first"), pl.col("price").last().alias("px_last"),
    ).drop("grp")


def detect_jumps(grid: pl.DataFrame, horizon_s: int, tick: float = TICK) -> pl.DataFrame:
    """Mid moves over `horizon_s` seconds on a 1-second as-of grid.

    `grid` has columns asset_id, win (strict window id), sec (whole seconds), mid.
    For horizon 1 every consecutive pair of seconds inside one window yields a
    move. For a longer horizon only seconds that are multiples of the horizon are
    compared with the second `horizon_s` earlier, so a single jump is counted once
    and not once per second it stays inside a sliding window.
    Returns rows (asset_id, win, sec, dmid, ticks) with ticks = |dmid| / tick."""
    s = grid.sort(["asset_id", "win", "sec"])
    s = s.with_columns(pl.col("sec").shift(horizon_s).over(["asset_id", "win"]).alias("sec_prev"),
                       pl.col("mid").shift(horizon_s).over(["asset_id", "win"]).alias("mid_prev"))
    # Why shift by horizon rows and check the second: the grid has one row per
    # second while a book is live, so row-shift == time-shift unless rows are
    # missing; the check on sec_prev drops any pair that spans a hole.
    out = s.filter((pl.col("sec_prev") == pl.col("sec") - horizon_s))
    if horizon_s > 1:
        out = out.filter(pl.col("sec") % horizon_s == 0)
    return out.with_columns((pl.col("mid") - pl.col("mid_prev")).alias("dmid")) \
        .with_columns(((pl.col("dmid").abs() / tick) + 1e-6).floor().cast(pl.Int64).alias("ticks")) \
        .select("asset_id", "win", "sec", "dmid", "ticks")


def implied_btc(ladder: pl.DataFrame, min_hours: float = 1.0) -> pl.DataFrame:
    """BTC price implied by a strike ladder, per minute: the strike at which the
    YES mid crosses 0.5, linearly interpolated between the two bracketing strikes.

    `ladder` columns: minute (epoch minutes), end_s (expiry), strike, mid.
    Why from the ladder and not from Binance: the laptop block has no spot feed
    and the VPS day has 16 minutes of it; the ladder itself says where the market
    puts BTC, which is exactly what "moneyness" should be measured against.
    Why prefer expiries at least `min_hours` away: close to expiry the ladder is
    a step function (0.01 / 0.99) and the interpolation has nothing to interpolate.
    Returns (minute, btc); minutes with no bracketing pair are absent."""
    if ladder.height == 0:
        return pl.DataFrame(schema={"minute": pl.Int64, "btc": pl.Float64})
    s = ladder.sort(["minute", "end_s", "strike"])
    s = s.with_columns(pl.col("strike").shift(-1).over(["minute", "end_s"]).alias("s_hi"),
                       pl.col("mid").shift(-1).over(["minute", "end_s"]).alias("m_hi"))
    br = s.filter((pl.col("mid") >= 0.5) & (pl.col("m_hi") < 0.5) & (pl.col("mid") > pl.col("m_hi")))
    br = br.with_columns((pl.col("strike") + (pl.col("mid") - 0.5) / (pl.col("mid") - pl.col("m_hi"))
                          * (pl.col("s_hi") - pl.col("strike"))).alias("btc"),
                         ((pl.col("end_s") - pl.col("minute") * 60) / 3600).alias("hte"))
    # one expiry per minute: nearest one that is still >= min_hours away, else the farthest
    br = br.with_columns(pl.when(pl.col("hte") >= min_hours).then(pl.col("hte")).otherwise(1e9).alias("rank"))
    return br.sort(["minute", "rank"]).group_by("minute", maintain_order=True).agg(pl.col("btc").first()) \
        .select("minute", "btc")


def mny_bucket(rel: float | None) -> str:
    if rel is None or not math.isfinite(rel):
        return "n/a"
    a = abs(rel)
    return "<0.5%" if a < 0.005 else "0.5-1%" if a < 0.01 else "1-2%" if a < 0.02 else "2-5%" if a < 0.05 else ">5%"


# ---- context: what each token is ---------------------------------------------------------

_STRIKE = re.compile(r"above-(\d+)(k?)-on-")
_BTC15 = re.compile(r"btc-updown-15m-(\d+)$")


def asset_context(assets: pl.DataFrame) -> pl.DataFrame:
    """Per asset: family, outcome_index, start_s (BTC 15 m window start), end_s
    (resolution instant), strike (ladder, in $)."""
    def strike(slug: str | None) -> float | None:
        m = _STRIKE.search(slug or "")
        return float(m.group(1)) * (1000 if m.group(2) else 1) if m else None

    def start_s(slug: str | None) -> int | None:
        m = _BTC15.search(slug or "")
        return int(m.group(1)) if m else None

    def end_s(s: str | None) -> int | None:
        if not s:
            return None
        try:
            return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
        except ValueError:
            return None

    return assets.with_columns(
        pl.col("market_slug").map_elements(strike, return_dtype=pl.Float64).alias("strike"),
        pl.col("market_slug").map_elements(start_s, return_dtype=pl.Int64).alias("start_s"),
        pl.col("end_date").map_elements(end_s, return_dtype=pl.Int64).alias("end_s"),
    ).select("asset_id", "condition_id", "family", "outcome_index", "sibling", "market_slug", "strike", "start_s", "end_s")


def bucket_sql(sec: str) -> str:
    """SQL for the time-to-expiry bucket of a row at whole second `sec` (needs
    columns family, start_s, end_s in scope)."""
    return f"""case
        when family = 'btc_15m' then (case when start_s is null then 'n/a'
            when {sec} < start_s then 'pre' when {sec} >= start_s + 900 then 'post'
            else 'm' || lpad(cast(({sec} - start_s) // 60 as varchar), 2, '0') end)
        when family = 'btc_daily_ladder' then (case when end_s is null then 'n/a'
            when end_s - {sec} < 0 then 'post' when end_s - {sec} < 3600 then 'h<1'
            when end_s - {sec} < 3 * 3600 then 'h1-3' when end_s - {sec} < 6 * 3600 then 'h3-6'
            when end_s - {sec} < 12 * 3600 then 'h6-12' when end_s - {sec} < 24 * 3600 then 'h12-24'
            when end_s - {sec} < 48 * 3600 then 'h24-48' else 'h>48' end)
        else 'flat' end"""


def _depth_sql(side: str, k: int, usd: bool) -> str:
    """Sum of level sizes (or $) on `side` within k ticks of mid, over the 5 stored levels.
    Why "within k ticks of mid" and not "k levels": a book with a hole at the
    second level is thinner than one without, and only a price-distance measure
    sees that. Level j counts if its price is at most k ticks from the mid; the
    1e-9 absorbs float noise. With 5 stored levels this is a lower bound when the
    book is dense and k = 5 (the 6th level could still be within 5 ticks)."""
    px = f"{side}_px_"
    cum = f"{side}_cum_"
    terms = []
    for j in range(1, 6):
        lvl = f"({cum}{j} - {cum}{j - 1})" if j > 1 else f"{cum}1"
        cond = f"{px}{j} >= mid - {k} * {TICK} - 1e-9" if side == "bid" else f"{px}{j} <= mid + {k} * {TICK} + 1e-9"
        val = f"{lvl} * {px}{j}" if usd else lvl
        terms.append(f"case when {px}{j} is not null and {cond} then {val} else 0 end")
    return " + ".join(terms)


# ---- views ------------------------------------------------------------------------------------


def open_shape(con: duckdb.DuckDBPyConnection, data_dir: Path, derived: Path, strict_path: Path, t0: int, t1: int,
               tmp_dir: Path | None = None) -> set[str]:
    """Register the raw inputs (`depth_1s`, `events_labeled`, `trades`, optional
    `spot_quotes`, `sw`, `ctx`) and build the analysis views. Returns the names present."""
    if tmp_dir is not None:
        tmp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"set temp_directory = '{tmp_dir}'")
    con.execute("set memory_limit = '2500MB'")
    con.execute("set preserve_insertion_order = false")
    have: set[str] = set()
    for name in ("depth_1s", "events_labeled"):
        files = sorted((derived / name).rglob("*.parquet"))
        if not files:
            raise RuntimeError(f"no {name} under {derived}")
        lst = ", ".join(f"'{f}'" for f in files)
        con.execute(f"create or replace view {name} as select * from read_parquet([{lst}], union_by_name=true)")
        have.add(name)
    raw = {"trades": ("events", "ts_recv_ns"), "spot_quotes": ("spot", "ts_recv_ns")}
    for name, (layer, tscol) in raw.items():
        files = _files(data_dir, layer, name, t0, t1)
        if not files:
            continue
        lst = ", ".join(f"'{f}'" for f in files)
        con.execute(f"create or replace view {name} as select * from read_parquet([{lst}], union_by_name=true) "
                    f"where {tscol} >= {t0} and {tscol} < {t1}")
        have.add(name)
    sw = pl.read_parquet(strict_path).select("asset_id", "t_start", "t_end")
    register_frames(con, sw=sw, ctx=asset_context(load_assets(data_dir)), rewards=load_rewards(data_dir))
    have.update({"sw", "ctx", "rewards"})
    define_views(con)
    return have


def define_views(con: duckdb.DuckDBPyConnection) -> None:
    """Analysis views on top of `depth_1s`, `events_labeled`, `trades`, `sw`, `ctx`.

    `swc`: strict windows with a window id and the token's context.
    `grid0`: depth_1s rows of outcome-0 tokens inside strict windows, with spread,
             best queues, depth within k ticks, empty-side flags, expiry bucket.
    `ev`:    adds and cancels of outcome-0 tokens inside strict windows.
    `evd`:   size decreases of ALL tokens inside strict windows (for trade-vs-queue).
    `tr`:    trades of all tokens inside their own strict windows, YES-frame side.
    Why ASOF joins: a token's strict windows are disjoint and sorted, so the
    window with the latest t_start <= ts is the only candidate; ASOF finds it in
    one pass instead of a range join."""
    con.execute("""
        create or replace view swc as
        select sw.asset_id, sw.t_start, sw.t_end, row_number() over (order by sw.asset_id, sw.t_start) as win,
               c.family, c.outcome_index, c.start_s, c.end_s, c.strike, c.condition_id
        from sw join ctx c using (asset_id)""")
    depth_cols = ", ".join(
        f"{_depth_sql(side, k, usd)} as d{'u' if usd else ''}_{side}_{k}"
        for side in ("bid", "ask") for k in DEPTH_K for usd in (False, True))
    con.execute(f"""
        create or replace view grid0 as
        with j as (
            select d.ts_recv_ns // {NS} as sec, d.asset_id, w.win, w.family, w.start_s, w.end_s, w.strike,
                   d.valid, d.n_bid_levels, d.n_ask_levels, d.bid_px_1, d.ask_px_1,
                   d.bid_cum_1, d.bid_cum_2, d.bid_cum_3, d.bid_cum_4, d.bid_cum_5,
                   d.ask_cum_1, d.ask_cum_2, d.ask_cum_3, d.ask_cum_4, d.ask_cum_5,
                   d.bid_px_2, d.bid_px_3, d.bid_px_4, d.bid_px_5, d.ask_px_2, d.ask_px_3, d.ask_px_4, d.ask_px_5
            from depth_1s d asof join swc w on d.asset_id = w.asset_id and d.ts_recv_ns >= w.t_start
            where d.ts_recv_ns <= w.t_end and w.outcome_index = 0),
        m as (select *, case when valid then (bid_px_1 + ask_px_1) / 2 end as mid,
                     case when valid then ask_px_1 - bid_px_1 end as spread from j)
        select sec, asset_id, win, family, strike, start_s, end_s, valid, mid, spread,
               case when valid then round(spread / {TICK}, 1) end as spread_ticks,
               n_bid_levels = 0 as empty_bid, n_ask_levels = 0 as empty_ask,
               bid_px_1, ask_px_1, bid_cum_1 as q_bid, ask_cum_1 as q_ask,
               bid_cum_1 * bid_px_1 as qu_bid, ask_cum_1 * ask_px_1 as qu_ask,
               {depth_cols},
               {bucket_sql('sec')} as bucket
        from m""")
    con.execute("""
        create or replace view ev as
        select e.ts_recv_ns as ts, e.asset_id, w.win, w.family, w.start_s, w.end_s, e.side, e.price,
               e.size_before, e.size_after, e.size_change, e.label
        from events_labeled e asof join swc w on e.asset_id = w.asset_id and e.ts_recv_ns >= w.t_start
        where e.ts_recv_ns <= w.t_end and w.outcome_index = 0 and e.label in ('add', 'cancel')""")
    con.execute("""
        create or replace view evd as
        select e.ts_recv_ns as ts, e.asset_id, e.side, e.price, e.size_before, e.size_change
        from events_labeled e asof join swc w on e.asset_id = w.asset_id and e.ts_recv_ns >= w.t_start
        where e.ts_recv_ns <= w.t_end and e.size_change < 0""")
    if "trades" in _views(con):
        con.execute(f"""
            create or replace view tr as
            select t.ts_recv_ns, t.asset_id, w.win, w.family, w.start_s, w.end_s, w.strike, w.condition_id,
                   t.price, t.size, t.side, t.size * t.price as usd,
                   (t.side = 'BUY') = (w.outcome_index = 0) as yes_buy,
                   case when w.outcome_index = 0 then t.price else 1 - t.price end as yes_price,
                   {bucket_sql(f't.ts_recv_ns // {NS}')} as bucket
            from trades t asof join swc w on t.asset_id = w.asset_id and t.ts_recv_ns >= w.t_start
            where t.ts_recv_ns <= w.t_end""")


def load_rewards(data_dir: Path) -> pl.DataFrame:
    """asset_id -> rewards_min_size, rewards_max_spread, rate_per_day (latest non-null
    value in the markets table). Why separate from `load_assets`: quality does not
    need these; the per-market table does (a reward pool changes what a quote is worth)."""
    files = sorted((data_dir / "reference" / "markets").rglob("*.parquet"))
    schema = {"asset_id": pl.String, "rewards_min_size": pl.Float64, "rewards_max_spread": pl.Float64, "rate_per_day": pl.Float64}
    if not files:
        return pl.DataFrame(schema=schema)
    cols = ["asset_id", "fetched_at_ns", "rewards_min_size", "rewards_max_spread", "rate_per_day"]
    parts = [pl.read_parquet(f, columns=[c for c in cols if c in pl.read_parquet_schema(f)]) for f in files]
    df = pl.concat(parts, how="diagonal_relaxed")
    for c in cols[2:]:
        if c not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(c))
    return df.sort("fetched_at_ns").group_by("asset_id").agg(
        pl.col("rewards_min_size").drop_nulls().last(), pl.col("rewards_max_spread").drop_nulls().last(),
        pl.col("rate_per_day").drop_nulls().last())


def market_stats(con: duckdb.DuckDBPyConnection, family: str, orders: pl.DataFrame) -> pl.DataFrame:
    """One row per market of `family` (outcome-0 token): where its price sits, its
    spread, depth and best queue, taker orders per minute, and its reward pool."""
    m = _df(con, f"""
        select g.asset_id, c.market_slug, count(*) as n_all, count(*) filter (where valid) as n_s,
               round({_Q.format(c='mid', q=0.5)}, 3) as mid_p50, round({_Q.format(c='spread_ticks', q=0.5)}, 1) as spread_p50_ticks,
               round(avg(case when spread_ticks >= 1 and spread_ticks < 2 then 1.0 else 0 end) filter (where valid), 4) as sh_1tick,
               round(avg(case when spread_ticks < 1 then 1.0 else 0 end) filter (where valid), 4) as sh_lt1,
               round({_Q.format(c='du_bid_3', q=0.5)}, 0) as depth3_bid_usd_p50, round({_Q.format(c='du_ask_3', q=0.5)}, 0) as depth3_ask_usd_p50,
               round({_Q.format(c='qu_bid', q=0.5)}, 0) as best_bid_usd_p50, round({_Q.format(c='qu_ask', q=0.5)}, 0) as best_ask_usd_p50,
               round(avg(case when empty_bid or empty_ask then 1.0 else 0 end), 4) as sh_empty_side
        from grid g join ctx c using (asset_id) where g.family = '{family}' group by all order by mid_p50 desc""")
    if m.height == 0:
        return m
    if orders.height:
        o = orders.join(_df(con, "select asset_id, condition_id from ctx"), on="asset_id", how="left")
        cond = _df(con, "select asset_id, condition_id from ctx")
        # Why by condition: taker orders print on the taker's token, so both tokens
        # of a market are folded onto its outcome-0 row.
        oc = o.group_by("condition_id").agg(pl.len().alias("n_orders"), pl.col("usd").median().round(1).alias("order_usd_p50"))
        m = m.join(cond, on="asset_id", how="left").join(oc, on="condition_id", how="left").drop("condition_id") \
            .with_columns((pl.col("n_orders") / (pl.col("n_all") / 60)).round(3).alias("orders_per_min"))
    if "rewards" in _views(con) or "rewards" in {r[0] for r in con.execute("select table_name from duckdb_tables()").fetchall()}:
        m = m.join(_df(con, "select * from rewards"), on="asset_id", how="left")
    return m.with_columns(pl.col("market_slug").str.slice(0, 48).alias("market")).drop("asset_id", "market_slug")


def _views(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {r[0] for r in con.execute("select view_name from duckdb_views() where not internal").fetchall()}


def _df(con: duckdb.DuckDBPyConnection, sql: str) -> pl.DataFrame:
    return con.sql(sql).pl()


def add_moneyness(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Build `grid` = `grid0` + `mny` (moneyness bucket of ladder strikes) and
    `btc_1m` (implied BTC per minute, Binance where recorded). Returns btc_1m."""
    lad = _df(con, """
        select sec // 60 as minute, end_s, strike, median(mid) as mid
        from grid0 where family = 'btc_daily_ladder' and valid and strike is not null and end_s is not null
        group by all""")
    btc = implied_btc(lad).with_columns(pl.lit("ladder").alias("src"))
    if "spot_quotes" in _views(con):
        bz = _df(con, f"""
            select ts_recv_ns // {60 * NS} as minute, avg((bid + ask) / 2) as btc
            from spot_quotes where bid is not null and bid > 0 group by 1""").with_columns(pl.lit("binance").alias("src"))
        # Why Binance wins where present: it is the actual reference price; the
        # ladder-implied price is the fallback for minutes without spot data.
        btc = pl.concat([bz, btc.filter(~pl.col("minute").is_in(bz["minute"]))])
    if btc.height == 0:
        btc = pl.DataFrame(schema={"minute": pl.Int64, "btc": pl.Float64, "src": pl.String})
    register_frames(con, btc_1m=btc.sort("minute"))
    con.execute("""
        create or replace view grid as
        select g.*, case when g.family <> 'btc_daily_ladder' or g.strike is null or b.btc is null then 'n/a'
                         when abs(g.strike / b.btc - 1) < 0.005 then '<0.5%'
                         when abs(g.strike / b.btc - 1) < 0.01 then '0.5-1%'
                         when abs(g.strike / b.btc - 1) < 0.02 then '1-2%'
                         when abs(g.strike / b.btc - 1) < 0.05 then '2-5%' else '>5%' end as mny
        from grid0 g left join btc_1m b on b.minute = g.sec // 60""")
    return btc


# ---- statistics ----------------------------------------------------------------------------------

_Q = "quantile_cont({c}, {q})"


def _qcols(col: str, qs=(0.25, 0.5, 0.75), prefix: str | None = None) -> str:
    p = prefix or col
    return ", ".join(f"round({_Q.format(c=col, q=q)}, 2) as {p}_p{int(q * 100)}" for q in qs)


def _share_cols() -> str:
    return ", ".join(
        f"round(avg(case when spread_ticks {cond} then 1.0 else 0 end), 4) as sh_{name}"
        for name, cond in (("lt1", "< 1"), ("1", ">= 1 and spread_ticks < 2"), ("2", ">= 2 and spread_ticks < 3"),
                           ("3", ">= 3 and spread_ticks < 4"), ("4_5", ">= 4 and spread_ticks <= 5"), ("gt5", "> 5")))


def spread_stats(con: duckdb.DuckDBPyConnection, by: str = "family") -> pl.DataFrame:
    """Time share of the spread per tick bin and spread quantiles, over valid grid
    seconds; plus the share of ALL grid seconds (valid or not) with one side empty.
    `by` = 'family' | 'bucket' | 'mny' (extra grouping column)."""
    grp = "family" if by == "family" else f"family, {by}"
    return _df(con, f"""
        with v as (select {grp}, count(*) as n_s, count(distinct asset_id) as n_tokens, {_share_cols()},
                          round({_Q.format(c='spread_ticks', q=0.5)}, 1) as p50_ticks,
                          round({_Q.format(c='spread_ticks', q=0.9)}, 1) as p90_ticks,
                          round(avg(spread_ticks), 2) as mean_ticks
                   from grid where valid group by all),
             a as (select {grp}, count(*) as n_all,
                          round(avg(case when empty_bid or empty_ask then 1.0 else 0 end), 4) as sh_empty_side
                   from grid group by all)
        select v.*, a.n_all, a.sh_empty_side from v join a using ({grp}) order by {grp}""")


def depth_stats(con: duckdb.DuckDBPyConnection, by: str = "family") -> pl.DataFrame:
    """Time-weighted quantiles of the cumulative size within k ticks of mid, per
    side, in shares and $. Valid grid seconds only."""
    grp = "family" if by == "family" else f"family, {by}"
    parts = []
    for side in ("bid", "ask"):
        for k in DEPTH_K:
            parts.append(f"""select {grp}, '{side}' as side, {k} as k, count(*) as n_s,
                {_qcols(f'd_{side}_{k}', prefix='sh')}, {_qcols(f'du_{side}_{k}', prefix='usd')}
                from grid where valid group by all""")
    return _df(con, " union all ".join(parts) + f" order by {grp}, side, k")


def imbalance_stats(con: duckdb.DuckDBPyConnection, k: int = 3) -> pl.DataFrame:
    """Distribution of (bid - ask) / (bid + ask) depth within k ticks, per family."""
    return _df(con, f"""
        with x as (select family, (d_bid_{k} - d_ask_{k}) / nullif(d_bid_{k} + d_ask_{k}, 0) as imb from grid where valid)
        select family, count(imb) as n_s, {_qcols('imb', (0.1, 0.25, 0.5, 0.75, 0.9))},
               round(avg(case when abs(imb) > 0.5 then 1.0 else 0 end), 4) as sh_abs_gt_05
        from x group by 1 order by 1""")


def queue_stats(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Size resting at the best bid / best ask (shares, $), time-weighted quantiles."""
    return _df(con, f"""
        select family, side, count(*) as n_s, {_qcols('q', (0.1, 0.25, 0.5, 0.75, 0.9), 'sh')},
               {_qcols('qu', (0.25, 0.5, 0.75), 'usd')}
        from (select family, 'bid' as side, q_bid as q, qu_bid as qu from grid where valid
              union all select family, 'ask', q_ask, qu_ask from grid where valid)
        group by all order by 1, 2""")


def trade_vs_queue(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """How much of the queue at its price a trade message consumes.

    Each trade is matched with the nearest size decrease at its price on the side
    it consumed (taker BUY eats the SELL/ask side), within ±1 s, own token; the
    level's `size_before` is the queue the taker found. Why not `label = 'fill'`:
    the labeller over-assigns it (quality report §4 item 2); the nearest decrease
    at the exact price is the one the trade caused in 99.9 % of cases."""
    if "tr" not in _views(con):
        return pl.DataFrame()
    return _df(con, f"""
        with m as (
            select t.family, t.size, t.usd, e.size_before, abs(e.ts - t.ts_recv_ns) as lag,
                   row_number() over (partition by t.ts_recv_ns, t.asset_id, t.price, t.size order by abs(e.ts - t.ts_recv_ns)) as rn
            from tr t join evd e on e.asset_id = t.asset_id and e.price = t.price
                 and e.side = case when t.side = 'BUY' then 'SELL' else 'BUY' end
                 and e.ts between t.ts_recv_ns - {NS} and t.ts_recv_ns + {NS}),
        q as (select family, size, usd, size_before, size / nullif(size_before, 0) as frac from m where rn = 1),
        n as (select family, count(*) as n_trades from tr group by 1)
        select q.family, n.n_trades, count(*) as n_matched, round(count(*) * 1.0 / n.n_trades, 4) as sh_matched,
               round({_Q.format(c='size_before', q=0.5)}, 1) as queue_hit_p50_sh,
               round({_Q.format(c='frac', q=0.25)}, 3) as frac_p25, round({_Q.format(c='frac', q=0.5)}, 3) as frac_p50,
               round({_Q.format(c='frac', q=0.75)}, 3) as frac_p75,
               round(avg(case when frac >= 0.999 then 1.0 else 0 end), 4) as sh_level_wiped
        from q join n using (family) group by q.family, n.n_trades order by 1""")


def flow_stats(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Adds and cancels per market-minute (count and shares) inside strict windows,
    per family and expiry bucket. Denominator = clean seconds of outcome-0 tokens
    on the grid (`n_s`). Why one query for all buckets: `ev` is a 20 M-row ASOF
    join over `events_labeled`; the family totals are sums of the bucket rows
    (`flow_family_from_bucket`), so the table is scanned once, not twice."""
    df = _df(con, f"""
        with s as (select family, bucket, count(*) as n_s from grid group by all),
             e as (select family, bucket, count(*) filter (where label = 'add') as n_add,
                          count(*) filter (where label = 'cancel') as n_cancel,
                          sum(size_change) filter (where label = 'add') as sh_add,
                          -sum(size_change) filter (where label = 'cancel') as sh_cancel
                   from (select *, {bucket_sql(f'ts // {NS}')} as bucket from ev) group by all)
        select s.family, s.bucket, s.n_s, coalesce(e.n_add, 0) as n_add, coalesce(e.n_cancel, 0) as n_cancel,
               coalesce(e.sh_add, 0) as sh_add, coalesce(e.sh_cancel, 0) as sh_cancel
        from s left join e using (family, bucket) order by 1, 2""")
    return _flow_rates(df)


def _flow_rates(df: pl.DataFrame) -> pl.DataFrame:
    m = pl.col("n_s") / 60.0
    return df.with_columns(m.round(1).alias("market_min"), (pl.col("n_add") / m).round(2).alias("adds_per_min"),
                           (pl.col("n_cancel") / m).round(2).alias("cancels_per_min"),
                           (pl.col("sh_add") / m).round(1).alias("add_sh_per_min"), (pl.col("sh_cancel") / m).round(1).alias("cancel_sh_per_min"))


def flow_family_from_bucket(byb: pl.DataFrame) -> pl.DataFrame:
    return _flow_rates(byb.group_by("family").agg(pl.col("n_s").sum(), pl.col("n_add").sum(), pl.col("n_cancel").sum(),
                                                  pl.col("sh_add").sum(), pl.col("sh_cancel").sum()).sort("family"))


def cancel_horizon_stats(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Resting-time proxy: share of added size cancelled at the same price and
    side within 1 / 10 / 60 s of the add.

    Polymarket's book is L2, so an individual order cannot be followed; the proxy
    attributes to each add the cancels that follow at its level, capped at the
    added size. Two adds in quick succession can both claim the same cancel, so
    this is an UPPER bound on "my order would have been pulled". Implemented with
    a running sum of cancelled size per level and a RANGE window frame, one pass
    per horizon, no self-join."""
    frames = ", ".join(
        f"max(cum_cancel) over (partition by asset_id, side, price order by ts range between current row and {h * NS} following) - cum_cancel as c{h}"
        for h in CANCEL_HORIZONS_S)
    cols = ", ".join(f"round(sum(least(add, c{h})) / sum(add), 4) as cancelled_within_{h}s" for h in CANCEL_HORIZONS_S)
    return _df(con, f"""
        with b as (select family, asset_id, side, price, ts, label, size_change,
                          sum(case when label = 'cancel' then -size_change else 0 end)
                              over (partition by asset_id, side, price order by ts rows unbounded preceding) as cum_cancel
                   from ev),
             w as (select family, label, size_change as add, {frames} from b),
             a as (select * from w where label = 'add')
        select family, count(*) as n_adds, round(sum(add), 0) as added_sh, {cols}
        from a group by 1 order by 1""")


def trade_stats(con: duckdb.DuckDBPyConnection) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """(per family, per bucket, taker orders frame). Sizes per trade message and
    per taker order (sweep-grouped), taker direction in the YES frame, sweeps."""
    if "tr" not in _views(con):
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()
    tr = _df(con, "select ts_recv_ns, asset_id, family, bucket, price, size, usd, side, yes_buy from tr")
    orders = group_sweeps(tr.select("ts_recv_ns", "asset_id", "side", "price", "size"))
    ctx = tr.group_by(["asset_id", "side"]).agg(pl.col("family").first())
    orders = orders.join(ctx, on=["asset_id", "side"], how="left")
    # bucket of an order = bucket of its first message
    orders = orders.join(tr.select("ts_recv_ns", "asset_id", "side", "bucket", "yes_buy").rename({"ts_recv_ns": "ts"}),
                         on=["ts", "asset_id", "side"], how="left").unique(subset=["ts", "asset_id", "side"])
    secs = _df(con, "select family, bucket, count(*) as n_s from grid group by all")
    fam_s = secs.group_by("family").agg(pl.col("n_s").sum())
    q = lambda c, p: pl.col(c).quantile(p, interpolation="linear")  # noqa: E731
    msg = tr.group_by("family").agg(
        pl.len().alias("n_msgs"), q("size", .25).round(1).alias("msg_sh_p25"), q("size", .5).round(1).alias("msg_sh_p50"),
        q("size", .75).round(1).alias("msg_sh_p75"), q("size", .9).round(1).alias("msg_sh_p90"),
        q("usd", .5).round(2).alias("msg_usd_p50"), q("usd", .9).round(2).alias("msg_usd_p90"),
        pl.col("yes_buy").mean().round(4).alias("yes_buy_sh_count"),
        ((pl.col("size") * pl.col("yes_buy")).sum() / pl.col("size").sum()).round(4).alias("yes_buy_sh_volume"),
        pl.col("size").sum().round(0).alias("volume_sh"), pl.col("usd").sum().round(0).alias("volume_usd"))
    od = orders.group_by("family").agg(
        pl.len().alias("n_orders"), q("size", .5).round(1).alias("order_sh_p50"), q("size", .9).round(1).alias("order_sh_p90"),
        q("usd", .5).round(2).alias("order_usd_p50"), q("usd", .9).round(2).alias("order_usd_p90"), q("usd", .99).round(0).alias("order_usd_p99"),
        (pl.col("n_msgs") >= 2).mean().round(4).alias("sh_multi_msg"),
        (pl.col("n_levels") >= 2).mean().round(4).alias("sh_multi_level"),
        (pl.col("size").filter(pl.col("n_levels") >= 2).sum() / pl.col("size").sum()).round(4).alias("sh_volume_multi_level"),
        pl.col("depth_ticks").filter(pl.col("n_levels") >= 2).quantile(.5).alias("sweep_ticks_p50"),
        pl.col("depth_ticks").filter(pl.col("n_levels") >= 2).quantile(.9).alias("sweep_ticks_p90"),
        pl.col("depth_ticks").max().alias("sweep_ticks_max"))
    fam = msg.join(od, on="family", how="full", coalesce=True).join(fam_s, on="family", how="left") \
        .with_columns((pl.col("n_orders") / (pl.col("n_s") / 60)).round(3).alias("orders_per_min"),
                      (pl.col("n_msgs") / (pl.col("n_s") / 60)).round(3).alias("msgs_per_min")).sort("family")
    byb = orders.group_by(["family", "bucket"]).agg(pl.len().alias("n_orders"), q("usd", .5).round(2).alias("order_usd_p50"),
                                                    pl.col("yes_buy").mean().round(3).alias("yes_buy_sh"),
                                                    (pl.col("n_levels") >= 2).mean().round(3).alias("sh_multi_level")) \
        .join(secs, on=["family", "bucket"], how="full", coalesce=True) \
        .with_columns((pl.col("n_orders") / (pl.col("n_s") / 60)).round(3).alias("orders_per_min")).sort(["family", "bucket"])
    return fam, byb, orders


def jump_stats(con: duckdb.DuckDBPyConnection) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """(per family, per bucket, size histogram of 1 s jumps, Binance coincidence)."""
    g = _df(con, "select asset_id, win, sec, mid, family, bucket from grid where valid")
    secs = g.group_by(["family", "bucket"]).agg(pl.len().alias("n_s"))
    fam_s = secs.group_by("family").agg(pl.col("n_s").sum())
    ctx = g.select("asset_id", "win", "sec", "family", "bucket")
    per_fam, per_b, hist = [], [], None
    j1 = None
    for h in JUMP_HORIZONS_S:
        j = detect_jumps(g.select("asset_id", "win", "sec", "mid"), h).join(ctx, on=["asset_id", "win", "sec"], how="left")
        if h == 1:
            j1 = j
            hist = j.filter(pl.col("ticks") >= JUMP_TICKS[0]).group_by(["family", "ticks"]).agg(pl.len().alias("n")).sort(["family", "ticks"])
        aggs = [(pl.col("ticks") >= k).sum().alias(f"n_{h}s_ge{k}") for k in JUMP_TICKS]
        per_fam.append(j.group_by("family").agg(pl.len().alias(f"n_pairs_{h}s"), *aggs))
        per_b.append(j.group_by(["family", "bucket"]).agg(pl.len().alias(f"n_pairs_{h}s"), *aggs))
    fam = per_fam[0]
    for f in per_fam[1:]:
        fam = fam.join(f, on="family", how="full", coalesce=True)
    fam = fam.join(fam_s, on="family", how="left").with_columns((pl.col("n_s") / 3600).round(2).alias("hours"))
    byb = per_b[0]
    for f in per_b[1:]:
        byb = byb.join(f, on=["family", "bucket"], how="full", coalesce=True)
    byb = byb.join(secs, on=["family", "bucket"], how="left").with_columns((pl.col("n_s") / 3600).round(3).alias("hours"))
    for h in JUMP_HORIZONS_S:
        for k in JUMP_TICKS:
            fam = fam.with_columns((pl.col(f"n_{h}s_ge{k}") / pl.col("hours")).round(2).alias(f"per_h_{h}s_ge{k}"))
            byb = byb.with_columns((pl.col(f"n_{h}s_ge{k}") / pl.col("hours")).round(2).alias(f"per_h_{h}s_ge{k}"))
    big = j1.filter(pl.col("ticks") >= JUMP_TICKS[0])
    sz = big.group_by("family").agg(pl.col("ticks").quantile(.5).alias("jump_ticks_p50"), pl.col("ticks").quantile(.9).alias("jump_ticks_p90"),
                                    pl.col("ticks").max().alias("jump_ticks_max"))
    fam = fam.join(sz, on="family", how="left").sort("family")
    bz = binance_coincidence(con, big) if "spot_quotes" in _views(con) else pl.DataFrame()
    return fam, byb.sort(["family", "bucket"]), hist, bz


def binance_range_1s(bz: pl.DataFrame, lookback_s: int = BINANCE_LOOKBACK_S) -> pl.DataFrame:
    """Per second: relative range (max - min) / min of the Binance mid over the
    preceding `lookback_s` seconds (inclusive). `bz` columns: sec, bmid (one row per second)."""
    s = bz.sort("sec").with_columns(
        pl.col("bmid").rolling_max(window_size=lookback_s + 1, min_samples=2).alias("hi"),
        pl.col("bmid").rolling_min(window_size=lookback_s + 1, min_samples=2).alias("lo"),
        pl.col("sec").shift(lookback_s).alias("sec_back"))
    return s.filter(pl.col("sec_back") == pl.col("sec") - lookback_s) \
        .with_columns((pl.col("hi") / pl.col("lo") - 1).alias("range")).select("sec", "range")


def binance_coincidence(con: duckdb.DuckDBPyConnection, jumps: pl.DataFrame) -> pl.DataFrame:
    """Share of Polymarket 1 s jumps (>= 2 ticks) preceded by a Binance range move
    in the previous 10 s, at two thresholds (0.05 % and 0.01 %), vs the base rate
    over all clean seconds with spot data; plus the range quantiles themselves.
    Why two thresholds: on a quiet Saturday BTC moves 0.05 % in 10 s less than
    0.1 % of the time, so the task's threshold alone would say nothing."""
    bz = _df(con, f"""
        select ts_recv_ns // {NS} as sec, arg_max((bid + ask) / 2, ts_recv_ns) as bmid
        from spot_quotes where bid is not null and bid > 0 group by 1 order by 1""")
    if bz.height < BINANCE_LOOKBACK_S + 2:
        return pl.DataFrame()
    # Why forward-fill onto a dense grid: bookTicker quotes arrive on every change,
    # and a quiet second must still carry the last known mid.
    dense = pl.DataFrame({"sec": pl.int_range(int(bz["sec"].min()), int(bz["sec"].max()) + 1, eager=True)})
    bz = dense.join(bz, on="sec", how="left").with_columns(pl.col("bmid").forward_fill())
    rng = binance_range_1s(bz).with_columns((pl.col("range") >= BINANCE_MOVE).alias("moved"),
                                            (pl.col("range") >= BINANCE_MOVE / 5).alias("moved_small"))
    g = _df(con, "select sec, family from grid where valid and family in ('btc_15m', 'btc_daily_ladder')")
    aggs = lambda n: [pl.len().alias(n), pl.col("moved").mean().round(4).alias(f"{n}_sh_ge005"),  # noqa: E731
                      pl.col("moved_small").mean().round(4).alias(f"{n}_sh_ge001"),
                      (pl.col("range").median() * 100).round(4).alias(f"{n}_range_p50_pct"),
                      (pl.col("range").quantile(0.9) * 100).round(4).alias(f"{n}_range_p90_pct")]
    base = g.join(rng, on="sec", how="inner").group_by("family").agg(*aggs("n_s"))
    jj = jumps.filter(pl.col("family").is_in(["btc_15m", "btc_daily_ladder"])).join(rng, on="sec", how="inner")
    res = jj.group_by("family").agg(*aggs("n_jumps"))
    return base.join(res, on="family", how="full", coalesce=True).sort("family")


def btc15_profile(con: duckdb.DuckDBPyConnection, orders: pl.DataFrame, jumps_b: pl.DataFrame) -> pl.DataFrame:
    """Life cycle of a 15-minute window, one row per minute 0-14 (+ pre)."""
    prof = _df(con, f"""
        select bucket, count(*) as n_s, count(distinct asset_id) as n_windows,
               round(avg(case when empty_bid or empty_ask then 1.0 else 0 end), 4) as sh_empty_side,
               round({_Q.format(c='spread_ticks', q=0.5)}, 1) as spread_p50_ticks,
               round(avg(case when spread_ticks >= 1 and spread_ticks < 2 then 1.0 else 0 end) filter (where valid), 4) as sh_1tick,
               round({_Q.format(c='du_bid_3 + du_ask_3', q=0.5)}, 0) as depth3_usd_p50,
               round({_Q.format(c='qu_bid + qu_ask', q=0.5)}, 0) as best_queue_usd_p50,
               round({_Q.format(c='abs(mid - 0.5)', q=0.5)}, 3) as abs_mid_minus_half_p50
        from grid where family = 'btc_15m' and bucket like 'm%' or (family = 'btc_15m' and bucket = 'pre') group by 1""")
    if orders.height:
        o = orders.filter(pl.col("family") == "btc_15m").group_by("bucket").agg(pl.len().alias("n_orders"), pl.col("usd").median().round(1).alias("order_usd_p50"))
        prof = prof.join(o, on="bucket", how="left").with_columns((pl.col("n_orders") / (pl.col("n_s") / 60)).round(3).alias("orders_per_min"))
    if jumps_b.height:
        jb = jumps_b.filter(pl.col("family") == "btc_15m").select("bucket", "per_h_1s_ge2", "per_h_1s_ge5", "per_h_10s_ge5")
        prof = prof.join(jb, on="bucket", how="left")
    order = {b: i for i, b in enumerate(BUCKET_ORDER)}
    return prof.with_columns(pl.col("bucket").replace_strict(order, default=99).alias("_o")).sort("_o").drop("_o")


# ---- report ---------------------------------------------------------------------------------------


def _fmt(v) -> str:
    if v is None:
        return "–"
    if isinstance(v, float):
        if math.isnan(v):
            return "–"
        return f"{v:,.4g}" if abs(v) < 1000 else f"{v:,.0f}"
    if isinstance(v, int):
        return f"{v:,}"
    return str(v)


def md_table(df: pl.DataFrame, cols: list[str] | None = None, rename: dict | None = None) -> str:
    if df.height == 0:
        return "_no data_"
    cols = cols or df.columns
    cols = [c for c in cols if c in df.columns]
    rename = rename or {}
    head = [rename.get(c, c) for c in cols]
    out = ["| " + " | ".join(head) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for row in df.select(cols).iter_rows():
        out.append("| " + " | ".join(_fmt(v) for v in row) + " |")
    return "\n".join(out)


def _sort_buckets(df: pl.DataFrame, col: str = "bucket") -> pl.DataFrame:
    if col not in df.columns:
        return df
    order = {b: i for i, b in enumerate(BUCKET_ORDER if col == "bucket" else MNY_ORDER)}
    keys = (["family"] if "family" in df.columns else []) + ["_o"]
    return df.with_columns(pl.col(col).replace_strict(order, default=99).alias("_o")).sort(keys).drop("_o")


def _pct(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    return df.with_columns([(pl.col(c) * 100).round(1).alias(c) for c in cols if c in df.columns])


def render_report(date: str, data_dir: Path, t0: int, t1: int, T: dict[str, pl.DataFrame], timings: dict[str, float],
                  notes: list[str]) -> str:
    f = lambda ns: datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")  # noqa: E731
    L = [f"# Shape of the market — {date}", "",
         f"Data root `{data_dir}`; window {f(t0)} → {f(t1)} UTC; statistics only inside strict clean windows "
         f"(`clean_windows_strict.parquet`), outcome-0 (Yes/Up) books, 1-second as-of grid (each second weighs the same). "
         f"Ticks are cents. **All numbers are provisional: one day of data.** Ran in {timings.get('total', 0):.0f} s.", ""]
    for n in notes:
        L.append(f"- {n}")
    L += ["", "## 1. Spread (time-weighted)", "",
          "Share of valid seconds by spread bin, quantiles in ticks, and share of all clean seconds with one side empty. "
          "n_s = clean seconds, n_tokens = outcome-0 tokens.", "",
          md_table(_pct(T["spread_family"], ["sh_lt1", "sh_1", "sh_2", "sh_3", "sh_4_5", "sh_gt5", "sh_empty_side"]),
                   ["family", "n_s", "n_all", "n_tokens", "sh_lt1", "sh_1", "sh_2", "sh_3", "sh_4_5", "sh_gt5", "p50_ticks", "p90_ticks", "mean_ticks", "sh_empty_side"],
                   {"n_all": "n_s (all)", "sh_lt1": "<1 %", "sh_1": "1 %", "sh_2": "2 %", "sh_3": "3 %", "sh_4_5": "4-5 %", "sh_gt5": ">5 %", "sh_empty_side": "empty side %"}),
          "", "![spread](spread.png)", "", "### Spread vs time to expiry", "",
          md_table(_pct(_sort_buckets(T["spread_bucket"]), ["sh_1", "sh_lt1", "sh_gt5", "sh_empty_side"]),
                   ["family", "bucket", "n_s", "n_tokens", "sh_lt1", "sh_1", "sh_gt5", "p50_ticks", "p90_ticks", "sh_empty_side"],
                   {"sh_lt1": "<1 %", "sh_1": "1 tick %", "sh_gt5": ">5 %", "sh_empty_side": "empty side %"}),
          "", "### Ladder: spread vs moneyness (|strike / BTC − 1|)", "",
          md_table(_pct(_sort_buckets(T["spread_mny"].filter(pl.col("family") == "btc_daily_ladder"), "mny"), ["sh_1", "sh_lt1", "sh_gt5", "sh_empty_side"]),
                   ["mny", "n_s", "n_tokens", "sh_lt1", "sh_1", "sh_gt5", "p50_ticks", "p90_ticks", "sh_empty_side"],
                   {"sh_lt1": "<1 %", "sh_1": "1 tick %", "sh_gt5": ">5 %", "sh_empty_side": "empty side %"}),
          "", "## 2. Depth within k ticks of mid (time-weighted, valid seconds)", "",
          "Cumulative resting size on each side whose price is within k ticks of the mid; $ = shares × price. "
          "5 stored levels, so k = 5 is a lower bound in dense books.", "",
          md_table(T["depth_family"], ["family", "side", "k", "n_s", "sh_p25", "sh_p50", "sh_p75", "usd_p25", "usd_p50", "usd_p75"]),
          "", "![depth](depth.png)", "", "### Depth within 3 ticks vs time to expiry (p50, bid + ask)", "",
          md_table(_sort_buckets(T["depth_bucket"]), ["family", "bucket", "n_s", "sh_p50", "usd_p50", "usd_p25", "usd_p75"]),
          "", "### Ladder: depth within 3 ticks vs moneyness (p50, bid + ask)", "",
          md_table(_sort_buckets(T["depth_mny"], "mny"), ["mny", "n_s", "sh_p50", "usd_p50", "usd_p25", "usd_p75"]),
          "", "### Depth imbalance (bid − ask) / (bid + ask) within 3 ticks", "",
          md_table(_pct(T["imbalance"], ["sh_abs_gt_05"]), None, {"sh_abs_gt_05": "|imb| > 0.5 %"}),
          "", "## 3. Queue at the best prices", "", "Size resting at the best bid / ask, time-weighted quantiles.", "",
          md_table(T["queue"], ["family", "side", "n_s", "sh_p10", "sh_p25", "sh_p50", "sh_p75", "sh_p90", "usd_p25", "usd_p50", "usd_p75"]),
          "", "Trade vs queue: each trade message matched with the nearest size decrease at its price on the consumed side (±1 s); "
          "`frac` = trade size / queue at that level before the trade; `sh_level_wiped` = share of trades that took the whole level.", "",
          md_table(_pct(T["trade_vs_queue"], ["sh_matched", "sh_level_wiped"]), None, {"sh_matched": "matched %", "sh_level_wiped": "level wiped %"}),
          "", "![queue](queue.png)", "", "## 4. Order flow (per market-minute of clean time)", "",
          md_table(T["flow_family"], ["family", "market_min", "n_add", "n_cancel", "adds_per_min", "cancels_per_min", "add_sh_per_min", "cancel_sh_per_min"]),
          "", "Taker orders per minute (trade messages grouped into orders by a 50 ms gap) and trade messages per minute:", "",
          md_table(T["trade_family"], ["family", "n_s", "n_msgs", "n_orders", "msgs_per_min", "orders_per_min"]),
          "", "Resting-time proxy (upper bound): share of added size cancelled at the same level within 1 / 10 / 60 s.", "",
          md_table(_pct(T["cancel_horizon"], [f"cancelled_within_{h}s" for h in CANCEL_HORIZONS_S]), None,
                   {f"cancelled_within_{h}s": f"≤{h} s %" for h in CANCEL_HORIZONS_S}),
          "", "![flow](flow.png)", "", "### Flow vs time to expiry", "",
          md_table(_sort_buckets(T["flow_bucket"].join(T["trade_bucket"].select("family", "bucket", "n_orders", "orders_per_min", "order_usd_p50", "yes_buy_sh", "sh_multi_level"),
                                                       on=["family", "bucket"], how="left")),
                   ["family", "bucket", "market_min", "adds_per_min", "cancels_per_min", "orders_per_min", "order_usd_p50", "yes_buy_sh", "sh_multi_level"]),
          "", "## 5. Trades", "", "Per trade message (one per maker hit) and per taker order (messages of one token+side within 50 ms). "
          "`yes_buy` = taker bought the Yes/Up token (or sold the No/Down one). Sweep = order that walked ≥ 2 price levels; "
          "`sweep_ticks` = levels spanned.", "",
          md_table(_pct(T["trade_family"], ["yes_buy_sh_count", "yes_buy_sh_volume", "sh_multi_msg", "sh_multi_level", "sh_volume_multi_level"]),
                   ["family", "n_msgs", "msg_sh_p25", "msg_sh_p50", "msg_sh_p75", "msg_sh_p90", "msg_usd_p50", "msg_usd_p90", "volume_sh", "volume_usd",
                    "yes_buy_sh_count", "yes_buy_sh_volume"],
                   {"yes_buy_sh_count": "yes buy % (count)", "yes_buy_sh_volume": "yes buy % (volume)"}),
          "", md_table(_pct(T["trade_family"], ["sh_multi_msg", "sh_multi_level", "sh_volume_multi_level"]),
                       ["family", "n_orders", "order_sh_p50", "order_sh_p90", "order_usd_p50", "order_usd_p90", "order_usd_p99", "sh_multi_msg", "sh_multi_level",
                        "sh_volume_multi_level", "sweep_ticks_p50", "sweep_ticks_p90", "sweep_ticks_max"],
                       {"sh_multi_msg": "multi-maker %", "sh_multi_level": "sweep %", "sh_volume_multi_level": "volume in sweeps %"}),
          "", "![trades](trades.png)", "", "## 6. Jumps of the mid (outcome-0 tokens, valid seconds)", "",
          "Moves of the 1-s grid mid over 1 s (consecutive seconds) and over 10 s (aligned, non-overlapping), per hour of clean time.", "",
          md_table(T["jump_family"], ["family", "hours", "n_pairs_1s", "per_h_1s_ge2", "per_h_1s_ge5", "per_h_1s_ge10", "per_h_10s_ge2", "per_h_10s_ge5", "per_h_10s_ge10",
                                      "jump_ticks_p50", "jump_ticks_p90", "jump_ticks_max"],
                   {"per_h_1s_ge2": "≥2t/1s per h", "per_h_1s_ge5": "≥5t/1s per h", "per_h_1s_ge10": "≥10t/1s per h",
                    "per_h_10s_ge2": "≥2t/10s per h", "per_h_10s_ge5": "≥5t/10s per h", "per_h_10s_ge10": "≥10t/10s per h"}),
          "", "### Jumps vs time to expiry (1 s moves per hour)", "",
          md_table(_sort_buckets(T["jump_bucket"]), ["family", "bucket", "hours", "per_h_1s_ge2", "per_h_1s_ge5", "per_h_1s_ge10", "per_h_10s_ge5"]),
          ""]
    if T.get("jump_binance") is not None and T["jump_binance"].height:
        L += ["### Binance coincidence (VPS data)", "",
              f"Share of 1 s jumps ≥ 2 ticks preceded by a Binance BTC/USDT range move ≥ 0.05 % (and ≥ 0.01 %) in the previous {BINANCE_LOOKBACK_S} s, "
              "vs the base rate over all clean seconds with spot data; `range` = (max − min) / min of the Binance mid over those 10 s, in %.", "",
              md_table(_pct(T["jump_binance"], ["n_s_sh_ge005", "n_s_sh_ge001", "n_jumps_sh_ge005", "n_jumps_sh_ge001"]), None,
                       {"n_s": "clean s with spot", "n_s_sh_ge005": "base ≥0.05 % (%)", "n_s_sh_ge001": "base ≥0.01 % (%)",
                        "n_s_range_p50_pct": "base range p50", "n_s_range_p90_pct": "base range p90",
                        "n_jumps_sh_ge005": "jumps ≥0.05 % (%)", "n_jumps_sh_ge001": "jumps ≥0.01 % (%)",
                        "n_jumps_range_p50_pct": "jump range p50", "n_jumps_range_p90_pct": "jump range p90"}), ""]
    else:
        L += ["Binance coincidence: no spot data in this window (laptop dataset).", ""]
    L += ["![jumps](jumps.png)", "", "## 7. BTC 15-minute life cycle (per minute of the window)", "",
          md_table(_pct(T["btc15_profile"], ["sh_empty_side", "sh_1tick"]),
                   ["bucket", "n_s", "n_windows", "spread_p50_ticks", "sh_1tick", "depth3_usd_p50", "best_queue_usd_p50", "orders_per_min", "order_usd_p50",
                    "abs_mid_minus_half_p50", "sh_empty_side", "per_h_1s_ge2", "per_h_1s_ge5", "per_h_10s_ge5"],
                   {"sh_1tick": "1 tick %", "sh_empty_side": "empty side %"}),
          "", "![btc15](btc15_lifecycle.png)", "", "![ladder](ladder.png)", "",
          "## 8. Fed December 2026: per market", "",
          "The five markets of the neg-risk event sit at very different prices; pooled numbers hide that. "
          "`rate_per_day` = daily reward pool ($), `rewards_max_spread` = max distance from mid (¢) to qualify.", "",
          md_table(_pct(T["fed_markets"], ["sh_1tick", "sh_lt1", "sh_empty_side"]),
                   ["market", "n_s", "mid_p50", "spread_p50_ticks", "sh_1tick", "sh_lt1", "depth3_bid_usd_p50", "depth3_ask_usd_p50",
                    "best_bid_usd_p50", "best_ask_usd_p50", "orders_per_min", "order_usd_p50", "rate_per_day", "rewards_max_spread", "rewards_min_size"],
                   {"sh_1tick": "1 tick %", "sh_lt1": "<1 tick %"}),
          "", "## Definitions", "",
          "- Clean time: strict windows `[t_start, last_verified]` from `polymm quality`; every table reads only inside them.",
          "- Grid: `depth_1s` as-of rows (book state at every whole second). A share of rows is a time share; quantiles are time-weighted.",
          "- Valid second: both sides quoted and not crossed. Spread/depth/queue use valid seconds; the empty-side share uses all clean seconds.",
          "- Bucket: btc_15m = minute within the live window (`pre` = subscribed before the window opened); ladder = hours to the 12:00 ET resolution; fed = flat.",
          "- Moneyness: |strike / BTC − 1| with BTC from Binance where recorded, else implied by the ladder (strike where the Yes mid crosses 0.5).",
          "- Taker order: trade messages of one token and taker side within 50 ms of each other (hashes are per message in this feed).",
          "- Jump: |mid(t) − mid(t − h)| in ticks on the grid, h = 1 s (every second) or 10 s (aligned blocks).",
          "- Resting proxy: cancels at the add's level within h s, capped at the add size; an upper bound (L2 book, no order ids).", ""]
    return "\n".join(L)


# ---- plots -------------------------------------------------------------------------------------------

# Why a fixed palette in family order: the same family must keep its colour on
# every figure and every day (dataviz rule: colour follows the entity, never the rank).
COLORS = {"btc_15m": "#2a78d6", "btc_daily_ladder": "#eb6834", "fed_dec_2026": "#1baf7a"}


def plot_all(T: dict[str, pl.DataFrame], out: Path) -> list[str]:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:  # Why not fail: the tables are the deliverable, plots are a view on them
        log.warning("matplotlib not installed; skipping plots (uv run --with matplotlib)")
        return []
    plt.rcParams.update({"figure.dpi": 110, "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True,
                         "grid.alpha": 0.25, "font.size": 9})
    done = []
    fams = [f for f in FAMILY_ORDER if f in T["spread_family"]["family"].to_list()]

    def save(fig, name):
        fig.tight_layout()
        fig.savefig(out / name)
        plt.close(fig)
        done.append(name)

    # 1 spread: stacked time shares per family
    sf = T["spread_family"]
    fig, ax = plt.subplots(figsize=(7, 3.2))
    bins = [("sh_lt1", "<1"), ("sh_1", "1"), ("sh_2", "2"), ("sh_3", "3"), ("sh_4_5", "4-5"), ("sh_gt5", ">5")]
    shades = ["#0b3d7a", "#2a78d6", "#6ea3e6", "#a9c7ef", "#d3e2f7", "#eef3fa"]
    bottom = [0.0] * len(fams)
    for (c, lab), col in zip(bins, shades):
        vals = [float(sf.filter(pl.col("family") == f)[c][0] or 0) * 100 for f in fams]
        ax.bar(fams, vals, bottom=bottom, color=col, edgecolor="white", linewidth=1, label=f"{lab} tick")
        for i, v in enumerate(vals):
            if v >= 6:
                ax.text(i, bottom[i] + v / 2, f"{v:.0f}%", ha="center", va="center", fontsize=8, color="black" if col in shades[2:] else "white")
        bottom = [b + v for b, v in zip(bottom, vals)]
    ax.set_ylabel("share of clean time (%)")
    ax.set_title("Spread in ticks, time-weighted (valid seconds)")
    ax.legend(ncol=6, fontsize=7, loc="upper center", bbox_to_anchor=(0.5, -0.12), frameon=False)
    save(fig, "spread.png")

    # 2 depth: $ within k ticks per side, p50 with p25-p75
    df = T["depth_family"]
    fig, axes = plt.subplots(1, 2, figsize=(8, 3.2), sharey=True)
    for ax, side in zip(axes, ("bid", "ask")):
        for f in fams:
            d = df.filter((pl.col("family") == f) & (pl.col("side") == side)).sort("k")
            if d.height == 0:
                continue
            ax.plot(d["k"], d["usd_p50"], marker="o", color=COLORS[f], label=f, linewidth=2)
            ax.fill_between(d["k"], d["usd_p25"], d["usd_p75"], color=COLORS[f], alpha=0.15, linewidth=0)
        ax.set_xlabel("within k ticks of mid")
        ax.set_yscale("log")
        ax.set_title(f"{side}: resting $ (p50, band p25–p75)")
        ax.set_xticks(list(DEPTH_K))
    axes[0].set_ylabel("$")
    axes[0].legend(frameon=False, fontsize=8)
    save(fig, "depth.png")

    # 3 queue: best-queue $ quantiles vs median trade $
    q = T["queue"]
    tq = T["trade_family"]
    fig, ax = plt.subplots(figsize=(7, 3.2))
    x = list(range(len(fams)))
    for i, f in enumerate(fams):
        for side, off in (("bid", -0.18), ("ask", 0.18)):
            d = q.filter((pl.col("family") == f) & (pl.col("side") == side))
            if d.height == 0:
                continue
            ax.plot([i + off, i + off], [d["usd_p25"][0], d["usd_p75"][0]], color=COLORS[f], linewidth=6, alpha=0.35, solid_capstyle="butt")
            ax.plot([i + off], [d["usd_p50"][0]], marker="_", markersize=14, color=COLORS[f], markeredgewidth=2)
        t = tq.filter(pl.col("family") == f)
        if t.height and t["msg_usd_p50"][0] is not None:
            ax.plot([i - 0.3, i + 0.3], [t["msg_usd_p50"][0]] * 2, color="#52514e", linestyle="--", linewidth=1.5)
            ax.text(i + 0.32, t["msg_usd_p50"][0], "median trade", fontsize=7, va="center", color="#52514e")
    ax.set_xticks(x)
    ax.set_xticklabels(fams)
    ax.set_yscale("log")
    ax.set_ylabel("$ at best price (bid left, ask right)")
    ax.set_title("Queue at the best prices: p25–p75 band, p50 tick, vs median trade $")
    save(fig, "queue.png")

    # 4 flow: rates per minute
    fl = T["flow_family"].join(T["trade_family"].select("family", "orders_per_min", "msgs_per_min"), on="family", how="left")
    fig, ax = plt.subplots(figsize=(7, 3.2))
    metrics = [("adds_per_min", "adds"), ("cancels_per_min", "cancels"), ("msgs_per_min", "trade msgs"), ("orders_per_min", "taker orders")]
    w = 0.2
    for j, (c, lab) in enumerate(metrics):
        vals = [float(fl.filter(pl.col("family") == f)[c][0] or 0) for f in fams]
        ax.bar([i + (j - 1.5) * w for i in range(len(fams))], vals, width=w * 0.92, color=["#0b3d7a", "#2a78d6", "#eb6834", "#f3a07e"][j], label=lab)
        for i, v in enumerate(vals):
            ax.text(i + (j - 1.5) * w, v * 1.15, f"{v:.3g}", ha="center", fontsize=7)
    ax.set_xticks(range(len(fams)))
    ax.set_xticklabels(fams)
    ax.set_yscale("log")
    ax.set_ylabel("per market-minute of clean time")
    ax.set_title("Order flow rates")
    ax.legend(frameon=False, fontsize=8, ncol=4)
    save(fig, "flow.png")

    # 5 trades: CDF of taker order $ per family
    orders = T.get("orders")
    fig, ax = plt.subplots(figsize=(7, 3.2))
    if orders is not None and orders.height:
        for f in fams:
            v = orders.filter(pl.col("family") == f)["usd"].sort().to_numpy()
            if len(v) == 0:
                continue
            import numpy as np
            ax.plot(v, np.arange(1, len(v) + 1) / len(v), color=COLORS[f], linewidth=2, label=f"{f} (n={len(v):,})")
        ax.set_xscale("log")
        ax.set_xlabel("taker order size ($)")
        ax.set_ylabel("CDF")
        ax.set_title("Taker order size")
        ax.legend(frameon=False, fontsize=8)
    save(fig, "trades.png")

    # 6 jumps: per hour by threshold
    jf = T["jump_family"]
    fig, axes = plt.subplots(1, 2, figsize=(8, 3.2), sharey=True)
    for ax, h in zip(axes, JUMP_HORIZONS_S):
        for i, f in enumerate(fams):
            d = jf.filter(pl.col("family") == f)
            if d.height == 0:
                continue
            ys = [float(d[f"per_h_{h}s_ge{k}"][0] or 0) for k in JUMP_TICKS]
            ax.plot(JUMP_TICKS, ys, marker="o", color=COLORS[f], linewidth=2, label=f)
        ax.set_xlabel("jump size ≥ ticks")
        ax.set_xticks(list(JUMP_TICKS))
        ax.set_yscale("symlog", linthresh=0.1)
        ax.set_title(f"mid moves within {h} s, per hour of clean time")
    axes[0].set_ylabel("jumps per hour")
    axes[0].legend(frameon=False, fontsize=8)
    save(fig, "jumps.png")

    # 7 btc15 life cycle
    p = T["btc15_profile"].filter(pl.col("bucket").str.starts_with("m"))
    fig, axes = plt.subplots(2, 3, figsize=(10, 5.2))
    # Why the 1-tick share and not the p50 spread: the p50 is pinned at 1 tick in
    # every minute; the share of time the spread is wider is where the signal is.
    p = p.with_columns((pl.col("sh_1tick") * 100).alias("pct_1tick"))
    panels = [("pct_1tick", "time at a 1-tick spread (%)"), ("depth3_usd_p50", "depth within 3 ticks, $ p50"),
              ("orders_per_min", "taker orders / min"), ("abs_mid_minus_half_p50", "|mid − 0.5| p50"),
              ("sh_empty_side", "share of seconds with a side empty"), ("per_h_1s_ge2", "jumps ≥ 2 ticks / 1 s, per hour")]
    if p.height:
        mins = [int(b[1:]) for b in p["bucket"].to_list()]
        for ax, (c, lab) in zip(axes.flat, panels):
            if c in p.columns:
                ax.plot(mins, p[c].to_list(), marker="o", color=COLORS["btc_15m"], linewidth=2)
            ax.set_title(lab, fontsize=9)
            ax.set_xlabel("minute of the 15-minute window")
            ax.set_xticks(range(0, 15, 2))
    fig.suptitle("BTC 15-minute window life cycle (btc_15m, clean seconds)")
    save(fig, "btc15_lifecycle.png")

    # 8 ladder: one measure per panel (no mixed axes): 1-tick time share and depth $,
    # each vs moneyness (top) and vs hours to expiry (bottom)
    fig, axes = plt.subplots(2, 2, figsize=(9, 5.6))
    sm = _sort_buckets(T["spread_mny"].filter(pl.col("family") == "btc_daily_ladder"), "mny")
    dm = _sort_buckets(T["depth_mny"], "mny")
    sb = _sort_buckets(T["spread_bucket"].filter(pl.col("family") == "btc_daily_ladder"))
    db = _sort_buckets(T["depth_bucket"].filter(pl.col("family") == "btc_daily_ladder"))
    for ax, df, x, y, title, scale in (
            (axes[0][0], sm, "mny", "sh_1", "time at a 1-tick spread (%) vs |strike / BTC − 1|", 100),
            (axes[0][1], dm, "mny", "usd_p50", "depth within 3 ticks, $ p50 vs |strike / BTC − 1|", 1),
            (axes[1][0], sb, "bucket", "sh_1", "time at a 1-tick spread (%) vs hours to 12:00 ET", 100),
            (axes[1][1], db, "bucket", "usd_p50", "depth within 3 ticks, $ p50 vs hours to 12:00 ET", 1)):
        if df.height and y in df.columns:
            ax.plot(df[x].to_list(), (df[y] * scale).to_list(), marker="o", color=COLORS["btc_daily_ladder"], linewidth=2)
            if y == "usd_p25" or "usd_p25" in df.columns and y == "usd_p50":
                ax.fill_between(df[x].to_list(), df["usd_p25"].to_list(), df["usd_p75"].to_list(), color=COLORS["btc_daily_ladder"], alpha=0.15, linewidth=0)
                ax.set_yscale("log")
        ax.set_title(title, fontsize=9)
    fig.suptitle("BTC daily ladder (btc_daily_ladder, clean seconds; band = p25–p75)")
    save(fig, "ladder.png")
    return done


# ---- driver ------------------------------------------------------------------------------------------


@dataclass
class ShapeConfig:
    date: str
    data_dir: Path
    out_dir: Path
    replay_out: Path | None = None  # default <quality_dir>/replay
    quality_dir: Path | None = None  # default reports/<date>; holds clean_windows_strict.parquet
    t0: int = 0
    t1: int = 0


def ensure_inputs(cfg: ShapeConfig) -> tuple[Path, Path, str]:
    """(derived_dir, strict_windows_path, note). Runs `polymm quality` (which runs
    the replay if needed) when the strict windows or the derived tables are missing."""
    qdir = cfg.quality_dir or Path("reports") / cfg.date
    rp_out = cfg.replay_out or (qdir / "replay")
    derived = rp_out / "derived"
    strict = qdir / "clean_windows_strict.parquet"
    ok = strict.exists() and all(any((derived / t).rglob("*.parquet")) for t in ("depth_1s", "events_labeled") if (derived / t).exists()) \
        and (derived / "depth_1s").exists() and (derived / "events_labeled").exists()
    if ok:
        return derived, strict, f"Replay reused from `{rp_out}`, strict windows from `{strict}`."
    log.info("quality/replay outputs missing; running polymm quality for %s", cfg.date)
    w0 = time.perf_counter()
    run_quality(QualityConfig(date=cfg.date, data_dir=cfg.data_dir, out_dir=qdir, replay_out=rp_out, t0=cfg.t0, t1=cfg.t1))
    return derived, strict, f"Ran `polymm quality` first ({time.perf_counter() - w0:.0f} s); replay under `{rp_out}`."


def run_shape(cfg: ShapeConfig) -> dict[str, pl.DataFrame]:
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    (cfg.out_dir / "tables").mkdir(exist_ok=True)
    derived, strict, note = ensure_inputs(cfg)
    con = duckdb.connect()
    w0 = time.perf_counter()
    have = open_shape(con, cfg.data_dir, derived, strict, cfg.t0, cfg.t1, tmp_dir=cfg.out_dir / "tmp")
    btc = add_moneyness(con)
    notes = [note, f"Binance spot: {'present' if 'spot_quotes' in have else 'absent'}; BTC reference minutes: "
                   f"{btc.filter(pl.col('src') == 'binance').height} Binance, {btc.filter(pl.col('src') == 'ladder').height} ladder-implied."]
    T: dict[str, pl.DataFrame] = {}
    timings: dict[str, float] = {}

    def step(name, fn):
        s0 = time.perf_counter()
        T[name] = fn()
        timings[name] = time.perf_counter() - s0
        log.info("%s: %.1f s", name, timings[name])

    step("spread_family", lambda: spread_stats(con))
    step("spread_bucket", lambda: spread_stats(con, "bucket"))
    step("spread_mny", lambda: spread_stats(con, "mny"))
    step("depth_family", lambda: depth_stats(con))
    step("depth_bucket", lambda: _df(con, f"""
        select family, bucket, count(*) as n_s, {_qcols('d_bid_3 + d_ask_3', (0.5,), 'sh')}, {_qcols('du_bid_3 + du_ask_3', prefix='usd')}
        from grid where valid group by all order by 1, 2"""))
    step("depth_mny", lambda: _df(con, f"""
        select mny, count(*) as n_s, {_qcols('d_bid_3 + d_ask_3', (0.5,), 'sh')}, {_qcols('du_bid_3 + du_ask_3', prefix='usd')}
        from grid where valid and family = 'btc_daily_ladder' group by all order by 1"""))
    step("imbalance", lambda: imbalance_stats(con))
    step("queue", lambda: queue_stats(con))
    step("trade_vs_queue", lambda: trade_vs_queue(con))
    step("flow_bucket", lambda: flow_stats(con))
    step("flow_family", lambda: flow_family_from_bucket(T["flow_bucket"]))
    step("cancel_horizon", lambda: cancel_horizon_stats(con))

    def trades():
        fam, byb, orders = trade_stats(con)
        T["trade_bucket"], T["orders"] = byb, orders
        return fam
    step("trade_family", trades)

    def jumps():
        fam, byb, hist, bz = jump_stats(con)
        T["jump_bucket"], T["jump_hist"], T["jump_binance"] = byb, hist, bz
        return fam
    step("jump_family", jumps)
    step("btc15_profile", lambda: btc15_profile(con, T["orders"], T["jump_bucket"]))
    step("fed_markets", lambda: market_stats(con, "fed_dec_2026", T["orders"]))
    T["btc_1m"] = btc
    timings["total"] = time.perf_counter() - w0
    for name, df in T.items():
        if isinstance(df, pl.DataFrame) and df.height:
            df.write_csv(cfg.out_dir / "tables" / f"{name}.csv")
    plots = plot_all(T, cfg.out_dir)
    log.info("plots: %s", ", ".join(plots) or "none")
    md = render_report(cfg.date, cfg.data_dir, cfg.t0, cfg.t1, T, timings, notes)
    (cfg.out_dir / "shape.md").write_text(md)
    con.close()
    shutil.rmtree(cfg.out_dir / "tmp", ignore_errors=True)
    return T
