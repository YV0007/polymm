"""Data-quality checks: is one recorded day trustworthy enough to analyse?

`polymm quality --date D` replays the day (or reuses an existing replay), then
runs every check below over the replay's derived tables (`tob`, `depth_1s`,
`events_labeled`, `clean_windows`) and the raw tables (`book_events`, `trades`,
`health`, `spot_*`), and writes

    reports/<date>/quality.md                  one page: check table + excluded list
    reports/<date>/clean_windows_strict.parquet windows trimmed to [t_start, last_verified]

Why a separate step from `replay`: replay answers "can the book be rebuilt?";
quality answers "which stretches of the day may an analysis use, and does the
data behave the way the exchange semantics say it should?" (books not crossed,
trades at the touch, YES/NO mirrored, clocks sane). Every later notebook starts
from `clean_windows_strict.parquet`, so a bad day is excluded once, here, not
rediscovered in each analysis.

Why DuckDB for the checks: `tob` is ~40 M rows per day. DuckDB streams Parquet
with projection pushdown and spills to disk, so the checks run on an 8 GB laptop
without loading a 3 GB frame; the few small results come back as polars frames.
Every check is a function of a DuckDB connection that holds views named like the
tables, so the tests can register tiny synthetic frames under the same names and
run the identical SQL.
"""
from __future__ import annotations

import json
import logging
import math
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import polars as pl

from .replay import NS, ReplayConfig, Replayer, _dates_covering

log = logging.getLogger(__name__)

# ---- thresholds ------------------------------------------------------------------
# Why one dict: thresholds are judgement calls that will move as we learn the
# feed; keeping them in one place (and in the report) makes a re-grade a one-line
# change and makes every PASS/WARN/FAIL traceable to a number.
THRESHOLDS: dict[str, float] = {
    # coverage: share of subscribed time inside strict clean windows, per family
    "coverage_strict_pass": 0.85,
    "coverage_strict_warn": 0.70,
    # Why lower for btc_15m: a token lives ~45 min and every window end loses up
    # to one REST anchor interval (10 min) of unverified tail; 85 % is unreachable
    # by construction, 60 % is what a healthy day delivers.
    "coverage_strict_pass_btc_15m": 0.60,
    "coverage_strict_warn_btc_15m": 0.40,
    "thin_hour_frac": 0.20,  # a full hour with < 20 % of the median hour's rows is "thin"
    # book sanity (inside loose clean windows)
    "crossed_seconds_warn": 60.0,  # time-weighted seconds with best_bid >= best_ask
    "crossed_seconds_fail": 600.0,
    "empty_side_time_warn": 0.01,  # share of window time with one side empty
    "empty_side_time_fail": 0.05,
    "spread_outlier": 0.20,  # spread above this is an outlier (fed/ladder tails are far narrower)
    "spread_outlier_time_warn": 0.01,
    # Why 180 s: in the last ~3 minutes of a 15 m window the price is 0.01 or 0.99
    # and nobody quotes the losing side (measured: all empty-side time sat in
    # minutes 12-14); before that a one-sided live book is a real anomaly.
    "live_window_tail_s": 180.0,
    # mirror rule
    "mirror_px_pass": 0.999,  # pre-A1: share of grid instants where YES bid == 1 - NO ask (and asks)
    "mirror_px_warn": 0.99,
    "mirror_kept_share_warn": 0.05,  # A1: kept outcome-1 deltas / outcome-0 deltas
    # trades vs book
    # Why not ~100 %: a taker order that walks several levels prints one trade
    # message per level; from the pre-sweep book the later ones sit above the ask
    # (or below the bid). They are consistent with the taker side, not errors.
    "trade_in_spread_pass": 0.90,
    "trade_in_spread_warn": 0.80,
    "trade_side_pass": 0.99,  # taker BUY at/above best ask, SELL at/below best bid
    "trade_side_warn": 0.95,
    "trade_wrong_side_warn": 0.005,  # BUY below best bid or SELL above best ask
    "trade_wrong_side_fail": 0.02,
    "trade_matched_pass": 0.95,  # trades with a fill-labelled delta within 1 s
    "trade_matched_warn": 0.80,
    # latency (recv - exchange ts on deltas)
    "latency_p50_warn_ms": 100.0,
    "latency_p50_fail_ms": 1000.0,
    "latency_p99_warn_ms": 2000.0,
    "latency_drift_warn_ms_per_h": 1.0,
    "latency_negative_share_warn": 0.0,  # any negative value is worth a look
    "latency_negative_share_fail": 0.01,
    # spot (Binance)
    "spot_gap_s": 5.0,
    "spot_latency_p99_warn_ms": 1000.0,
    "spot_direction_pass": 0.80,  # share of seconds where Binance direction agrees with the Up token
    # cross-feed clock
    "xcorr_lag_pass_min_s": 0,  # Binance must not lag Polymarket (would mean a timestamp problem)
    "xcorr_lag_pass_max_s": 15,
    "xcorr_min_corr": 0.05,
}

PASS, WARN, FAIL, SKIP = "PASS", "WARN", "FAIL", "SKIP"


@dataclass
class Check:
    section: str
    name: str
    family: str  # "all" or a family name
    value: float | None
    n: int
    threshold: str
    status: str
    fmt: str = "float"  # pct | ms | s | int | float | lag
    note: str = ""

    def value_str(self) -> str:
        v = self.value
        if v is None:
            return "–"
        if self.fmt == "pct":
            return f"{100 * v:.2f} %"
        if self.fmt == "ms":
            return f"{v:.1f} ms"
        if self.fmt == "s":
            return f"{v:.1f} s"
        if self.fmt == "int":
            return f"{int(v):,}"
        if self.fmt == "lag":
            return f"{v:+.0f} s"
        return f"{v:.4g}"


@dataclass
class Result:
    checks: list[Check] = field(default_factory=list)
    tables: dict[str, pl.DataFrame] = field(default_factory=dict)  # small detail tables for the report
    excluded: list[str] = field(default_factory=list)

    def extend(self, other: "Result") -> None:
        self.checks.extend(other.checks)
        self.tables.update(other.tables)
        self.excluded.extend(other.excluded)


def grade_hi(value: float | None, pass_at: float, warn_at: float) -> str:
    """Higher is better: PASS >= pass_at, WARN >= warn_at, else FAIL."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return SKIP
    return PASS if value >= pass_at else WARN if value >= warn_at else FAIL


def grade_lo(value: float | None, warn_above: float, fail_above: float) -> str:
    """Lower is better: PASS <= warn_above, WARN <= fail_above, else FAIL."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return SKIP
    return PASS if value <= warn_above else WARN if value <= fail_above else FAIL


# ---- data access -------------------------------------------------------------------


def _files(root: Path, layer: str, table: str, t0: int, t1: int) -> list[str]:
    """Parquet files of a raw table for the dt= partitions that can hold [t0, t1).

    Why by date and not by Parquet statistics (as replay does): the raw views are
    filtered on ts_recv_ns in SQL anyway, and DuckDB prunes row groups by its own
    min/max statistics; an extra day of files costs nothing measurable."""
    out: list[str] = []
    for d in _dates_covering(t0, t1):
        base = root / layer / table / f"dt={d}"
        if base.exists():
            out.extend(str(p) for p in sorted(base.rglob("*.parquet")))
    return out


def load_assets(data_dir: Path) -> pl.DataFrame:
    """asset_id -> condition_id, family, outcome_index, sibling, market_slug.

    Why not `replay.load_market_index` alone: the quality checks also need the
    slug (BTC 15-minute windows carry their start time in it) and the sibling
    token id in a frame that DuckDB can join on."""
    files = sorted((data_dir / "reference" / "markets").rglob("*.parquet"))
    cols = ["asset_id", "condition_id", "family", "outcome", "outcome_index", "market_slug", "end_date"]
    if not files:
        return pl.DataFrame(schema={"asset_id": pl.String, "condition_id": pl.String, "family": pl.String,
                                    "outcome_index": pl.Int32, "sibling": pl.String, "market_slug": pl.String,
                                    "end_date": pl.String})
    parts = []
    for f in files:
        have = pl.read_parquet_schema(f)
        parts.append(pl.read_parquet(f, columns=[c for c in cols if c in have]))
    df = pl.concat(parts, how="diagonal_relaxed")
    if "outcome_index" not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=pl.Int32).alias("outcome_index"))
    # Why the Yes/Up fallback: `outcome_index` exists only from 2026-11-07 (A1);
    # before that outcome 0 is Yes/Up by Polymarket's clobTokenIds convention.
    df = df.with_columns(
        pl.when(pl.col("outcome_index").is_not_null()).then(pl.col("outcome_index"))
        .when(pl.col("outcome").is_in(["Yes", "Up"])).then(0).otherwise(1).cast(pl.Int32).alias("oi"))
    agg = df.group_by("asset_id").agg(pl.col("condition_id").first(), pl.col("family").first(), pl.col("oi").max(),
                                      pl.col("market_slug").first(), pl.col("end_date").first())
    sib = agg.join(agg.select("condition_id", pl.col("asset_id").alias("sibling"), pl.col("oi").alias("oi2")),
                   on="condition_id", how="left").filter(pl.col("oi") != pl.col("oi2")) \
        .select("asset_id", "sibling").unique(subset=["asset_id"])
    return agg.join(sib, on="asset_id", how="left").rename({"oi": "outcome_index"}) \
        .select("asset_id", "condition_id", "family", "outcome_index", "sibling", "market_slug", "end_date")


def register_frames(con: duckdb.DuckDBPyConnection, **frames: pl.DataFrame) -> None:
    """Register polars frames as DuckDB views (tests, or any in-memory use)."""
    for name, df in frames.items():
        con.register(name, df.to_arrow())


def open_views(con: duckdb.DuckDBPyConnection, data_dir: Path, derived_dir: Path, t0: int, t1: int,
               tmp_dir: Path | None = None) -> set[str]:
    """Create the views the checks read. Returns the names that exist (a table
    with no files, e.g. `spot_quotes` on the laptop dataset, is simply absent)."""
    if tmp_dir is not None:
        tmp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"set temp_directory = '{tmp_dir}'")
    # Why a memory cap: the laptop has 8 GB and other things running; DuckDB spills
    # window functions and sorts to temp_directory instead of getting killed.
    con.execute("set memory_limit = '3GB'")
    con.execute("set preserve_insertion_order = false")
    have: set[str] = set()
    register_frames(con, assets=load_assets(data_dir))
    have.add("assets")
    raw = {"book_events": ("events", "ts_recv_ns"), "trades": ("events", "ts_recv_ns"), "health": ("events", "ts_ns"),
           "spot_quotes": ("spot", "ts_recv_ns"), "spot_trades": ("spot", "ts_recv_ns")}
    for name, (layer, tscol) in raw.items():
        files = _files(data_dir, layer, name, t0, t1)
        if not files:
            continue
        lst = ", ".join(f"'{f}'" for f in files)
        con.execute(f"create or replace view {name} as select * from read_parquet([{lst}], union_by_name=true) "
                    f"where {tscol} >= {t0} and {tscol} < {t1}")
        have.add(name)
    for name in ("tob", "depth_1s", "events_labeled", "clean_windows", "rest_checks"):
        files = sorted((derived_dir / name).rglob("*.parquet"))
        if not files:
            continue
        lst = ", ".join(f"'{f}'" for f in files)
        con.execute(f"create or replace view {name} as select * from read_parquet([{lst}], union_by_name=true)")
        have.add(name)
    if "clean_windows" in have:
        define_window_views(con, t0, t1)
        have.update({"span", "cw", "tob_cw"})
    return have


def define_window_views(con: duckdb.DuckDBPyConnection, t0: int, t1: int) -> None:
    """`cw`: clean windows clipped to [t0, t1) AND to the token's own event span,
    with `last_verified` inherited from the sibling token. `tob_cw`: tob rows
    inside a loose window with the time until the next row of that asset (`dur_ns`).

    Why clip to the event span: replay closes the last window of every token at
    t1 (`end_reason = end`), midnight for a full day, even for a BTC 15-minute
    token that went silent at 10:00 when it was unsubscribed. Time-weighted
    numbers would otherwise count 14 h of a book nobody was recording.
    Why inherit the sibling's verification: in the A1 era outcome 1 is rebuilt by
    mirroring outcome 0 and its redundant REST snapshots are not stored, so it
    never gets a verifying snapshot of its own; a verified outcome-0 book verifies
    the mirror (938/938 REST checks in replay_report §2). Pre-A1 both tokens are
    verified independently and the inheritance changes nothing.
    Why ASOF JOIN for tob: windows of one asset are disjoint and sorted, so "the
    window whose start is the latest one <= ts" is the only candidate, and ASOF
    finds it in one sorted pass instead of a range join over 40 M x 1 k rows.
    Why time weights: `tob` has a row per event, so counting rows over-weights
    busy minutes; `dur_ns` lets every check report seconds as well as rows."""
    # Why exclude reconnect rows from the span: replay writes a `reconnect` tob row
    # for every initialised book, including a 15-minute token unsubscribed an hour
    # earlier; its last REAL event is where the subscribed time ends.
    con.execute("""
        create or replace view span as
        select asset_id, min(ts_recv_ns) as s, max(ts_recv_ns) as e from tob where event_type <> 'reconnect' group by 1""")
    con.execute(f"""
        create or replace view cw as
        with w as (select w.asset_id, greatest(w.t_start, {t0}) as t_start, least(w.t_end, {t1}, span.e) as t_end, w.start_kind,
                     w.end_reason, case when w.last_verified is null then null else least(w.last_verified, {t1}) end as own_verified,
                     w.n_events
              from clean_windows w join span using (asset_id)
              where least(w.t_end, {t1}, span.e) > greatest(w.t_start, {t0})),
        w2 as (select w.asset_id, w.t_start, w.t_end, w.start_kind, w.end_reason, w.own_verified, w.n_events,
                      max(s2.own_verified) as sib_verified
               from w left join assets a on a.asset_id = w.asset_id
                      left join w s2 on s2.asset_id = a.sibling and s2.own_verified >= w.t_start and s2.own_verified < w.t_end
               group by all)
        select asset_id, t_start, t_end, start_kind, end_reason, own_verified, sib_verified,
               greatest(coalesce(own_verified, 0), coalesce(sib_verified, 0)) as lv
               , case when greatest(coalesce(own_verified, 0), coalesce(sib_verified, 0)) = 0 then null
                      else greatest(coalesce(own_verified, 0), coalesce(sib_verified, 0)) end as last_verified, n_events
        from w2""")
    con.execute("""
        create or replace view tob_cw as
        with j as (
            select t.ts_recv_ns as ts, t.asset_id, t.event_type, t.best_bid, t.bid_size, t.best_ask, t.ask_size,
                   t.spread, t.valid, w.t_start, w.t_end, w.last_verified
            from tob t asof join cw w on t.asset_id = w.asset_id and t.ts_recv_ns >= w.t_start
            where t.ts_recv_ns < w.t_end)
        select j.*, coalesce(lead(ts) over (partition by asset_id, t_start order by ts), t_end) - ts as dur_ns
        from j""")


def _df(con: duckdb.DuckDBPyConnection, sql: str) -> pl.DataFrame:
    return con.sql(sql).pl()


def wquantile(hist: pl.DataFrame, value: str, weight: str, q: float) -> float | None:
    """Quantile of a histogram (value, count). Why histograms: a day has ~40 M
    deltas; DuckDB's quantile_cont keeps every value per group in memory and ran
    out of it, while a histogram at 0.1 ms resolution is a few thousand rows and
    gives the same number to the resolution we report."""
    if hist.height == 0:
        return None
    h = hist.sort(value)
    cum = h[weight].cum_sum()
    total = float(cum[-1])
    if total <= 0:
        return None
    idx = int((cum >= q * total).arg_max())
    return float(h[value][idx])


def _slope(x: pl.Series, y: pl.Series) -> tuple[float | None, float | None]:
    """Least-squares slope of y on x and r²; None if fewer than 3 points."""
    if x.len() < 3 or x.var() in (None, 0):
        return None, None
    xm, ym = x.mean(), y.mean()
    cov = ((x - xm) * (y - ym)).sum() / (x.len() - 1)
    slope = cov / x.var()
    corr = cov / math.sqrt(x.var() * y.var()) if y.var() else 0.0
    return float(slope), float(corr * corr)


def _hour(ts_expr: str) -> str:
    return f"(({ts_expr}) // {3600 * NS}) * 3600"


def _fmt_hour(epoch_s: int | float) -> str:
    return datetime.fromtimestamp(int(epoch_s), tz=timezone.utc).strftime("%m-%d %H:00")


def _fmt_ts(ns: int | float) -> str:
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime("%H:%M:%S")


# ---- checks ----------------------------------------------------------------------------


def check_coverage(con: duckdb.DuckDBPyConnection, t0: int, t1: int, th: dict = THRESHOLDS) -> Result:
    """Share of subscribed time inside clean windows, per family, loose and strict.

    Subscribed time of a token = first to last tob row in the window (tob has a
    row per event, REST anchors every 10 min included, so a subscribed token is
    never silent for long). Loose = inside [t_start, t_end); strict = inside
    [t_start, last_verified], the part a REST or WS snapshot has confirmed."""
    r = Result()
    cov = _df(con, """
        with w as (select asset_id, sum(t_end - t_start) as loose,
                     sum(greatest(coalesce(last_verified, t_start) - t_start, 0)) as strict,
                     count(*) as nw, count(*) filter (where last_verified is null) as n_unverified
              from cw group by 1)
        select coalesce(a.family, 'unknown') as family, count(*) as n_assets,
               sum(e - s) / 1e9 as sub_s, sum(coalesce(loose, 0)) / 1e9 as loose_s,
               sum(coalesce(strict, 0)) / 1e9 as strict_s, sum(coalesce(nw, 0)) as n_windows,
               sum(coalesce(n_unverified, 0)) as n_unverified,
               count(*) filter (where nw is null) as n_no_window
        from span left join w using (asset_id) left join assets a using (asset_id) group by 1 order by 1""")
    tot = cov.select(pl.col("sub_s").sum(), pl.col("loose_s").sum(), pl.col("strict_s").sum(),
                     pl.col("n_windows").sum(), pl.col("n_unverified").sum(), pl.col("n_no_window").sum()).row(0)
    rows = [("all", *tot)] + [tuple(x) for x in cov.select("family", "sub_s", "loose_s", "strict_s", "n_windows",
                                                            "n_unverified", "n_no_window").iter_rows()]
    for fam, sub_s, loose_s, strict_s, nw, nunv, nnow in rows:
        loose = loose_s / sub_s if sub_s else None
        strict = strict_s / sub_s if sub_s else None
        r.checks.append(Check("coverage", "clean share (loose)", fam, loose, int(nw), "info", PASS if loose is not None else SKIP, "pct",
                              f"{loose_s / 3600:.1f} h of {sub_s / 3600:.1f} h subscribed"))
        p_at = th.get(f"coverage_strict_pass_{fam}", th["coverage_strict_pass"])
        w_at = th.get(f"coverage_strict_warn_{fam}", th["coverage_strict_warn"])
        r.checks.append(Check("coverage", "clean share (strict)", fam, strict, int(nw), f">= {p_at:.0%} / {w_at:.0%}",
                              grade_hi(strict, p_at, w_at), "pct",
                              f"{strict_s / 3600:.1f} h; {int(nunv)} windows never verified; {int(nnow)} tokens without a window"))
    hist = _df(con, """
        select end_reason, count(*) as n, round(sum(t_end - t_start) / 3.6e12, 2) as loose_h,
               round(sum(greatest(coalesce(last_verified, t_start) - t_start, 0)) / 3.6e12, 2) as strict_h,
               count(*) filter (where last_verified is null) as n_unverified
        from cw group by 1 order by 1""")
    r.tables["end_reason"] = hist
    for reason, n, lh, sh, nu in hist.iter_rows():
        r.checks.append(Check("coverage", f"windows ending by {reason}", "all", n, n, "info", PASS, "int",
                              f"{lh:.1f} h loose, {sh:.1f} h strict, {nu} unverified"))
    if "book_events" in _views(con):
        per_hour = _df(con, f"""
            select {_hour('ts_recv_ns')} as hour, count(*) as rows, count(distinct asset_id) as tokens,
                   count(*) filter (where event_type = 'delta') as deltas,
                   count(*) filter (where event_type = 'snapshot_ws') as snap_ws,
                   count(*) filter (where event_type = 'snapshot_rest') as snap_rest,
                   count(*) filter (where event_type = 'reconnect') as reconnects
            from book_events group by 1 order by 1""")
        # Why only full hours: the first/last hour of a window is short by construction.
        full = per_hour.filter((pl.col("hour") * NS >= t0) & ((pl.col("hour") + 3600) * NS <= t1))
        med = float(full["rows"].median()) if full.height else 0.0
        thin = full.filter(pl.col("rows") < th["thin_hour_frac"] * med) if med else full.head(0)
        per_hour = per_hour.with_columns(
            pl.when(pl.col("hour").is_in(thin["hour"].to_list())).then(pl.lit("thin")).otherwise(pl.lit("")).alias("flag"),
            pl.col("hour").map_elements(_fmt_hour, return_dtype=pl.String).alias("hour_utc")).drop("hour") \
            .select("hour_utc", "rows", "tokens", "deltas", "snap_ws", "snap_rest", "reconnects", "flag")
        r.tables["per_hour"] = per_hour
        r.checks.append(Check("coverage", "thin hours", "all", thin.height, full.height,
                              f"< {th['thin_hour_frac']:.0%} of median hour ({med:,.0f} rows)",
                              PASS if thin.height == 0 else WARN, "int",
                              ", ".join(_fmt_hour(h) for h in thin["hour"].to_list()) or "none"))
        for h, n in thin.select("hour", "rows").iter_rows():
            r.excluded.append(f"hour {_fmt_hour(h)} UTC: thin ({n:,} rows vs median {med:,.0f})")
    nunv = int(tot[4])
    if nunv:
        r.excluded.append(f"{nunv} clean windows with no verifying snapshot (strict length 0): "
                          f"{cov.filter(pl.col('n_unverified') > 0).select('family', 'n_unverified').rows()}")
    return r


def _views(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {r[0] for r in con.sql("select view_name from duckdb_views() where not internal").fetchall()}


def check_book_sanity(con: duckdb.DuckDBPyConnection, th: dict = THRESHOLDS) -> Result:
    """Inside loose clean windows: crossed books, empty sides, spread outliers.

    A crossed top of book (bid >= ask) cannot rest on a CLOB; inside a trusted
    window it is either a transient between two frames of one update or a bug.
    Reported as rows and time-weighted seconds; the seconds are what matter.

    Why two passes into small temp tables: `tob_cw` is a 40 M-row ASOF join plus
    a window function; evaluating it once per question cost 200 s. One pass
    aggregates everything per (asset, window), a second builds a spread
    histogram per family; every number below comes from those few rows."""
    r = Result()
    so = th["spread_outlier"]
    tail = th["live_window_tail_s"]
    _register_btc15(con)
    con.execute(f"""
        create or replace temp table _tob_agg as
        select asset_id, t_start, count(*) as n_rows, sum(dur_ns) as secs_ns,
               count(*) filter (where best_bid >= best_ask) as crossed_n,
               coalesce(sum(dur_ns) filter (where best_bid >= best_ask), 0) as crossed_ns,
               min(ts) filter (where best_bid >= best_ask) as first_crossed_ts,
               count(*) filter (where best_bid is null or best_ask is null) as empty_n,
               coalesce(sum(dur_ns) filter (where best_bid is null or best_ask is null), 0) as empty_ns,
               count(*) filter (where spread > {so}) as wide_n,
               coalesce(sum(dur_ns) filter (where spread > {so}), 0) as wide_ns,
               count(*) filter (where live) as live_n, coalesce(sum(dur_ns) filter (where live), 0) as live_ns,
               coalesce(sum(dur_ns) filter (where live and (best_bid is null or best_ask is null)), 0) as live_empty_ns,
               coalesce(sum(dur_ns) filter (where live and best_bid >= best_ask), 0) as live_crossed_ns
        from (select t.*, (w.start_s is not null and t.ts >= w.start_s * {NS} and t.ts < (w.end_s - {tail}) * {NS}) as live
              from tob_cw t left join _btc15 w using (asset_id)) t
        group by 1, 2""")
    con.execute("""
        create or replace temp table _spread_hist as
        select coalesce(a.family, 'unknown') as family, round(spread, 3) as sp, count(*) as n
        from tob_cw t left join assets a using (asset_id) where valid group by all""")
    df = _df(con, """
        select coalesce(a.family, 'unknown') as family, sum(n_rows) as n_rows, sum(secs_ns) / 1e9 as secs,
               sum(crossed_n) as crossed_n, sum(crossed_ns) / 1e9 as crossed_s, sum(empty_n) as empty_n, sum(empty_ns) / 1e9 as empty_s,
               sum(wide_n) as wide_n, sum(wide_ns) / 1e9 as wide_s, count(distinct asset_id) as n_assets
        from _tob_agg g left join assets a using (asset_id) group by 1 order by 1""")
    tot = df.select(pl.col("n_rows").sum(), pl.col("secs").sum(), pl.col("crossed_n").sum(), pl.col("crossed_s").sum(),
                    pl.col("empty_n").sum(), pl.col("empty_s").sum(), pl.col("wide_n").sum(), pl.col("wide_s").sum()).row(0)
    rows = [("all", *tot)] + [tuple(x) for x in df.select("family", "n_rows", "secs", "crossed_n", "crossed_s", "empty_n",
                                                           "empty_s", "wide_n", "wide_s").iter_rows()]
    for fam, n, secs, cn, cs, en, es, wn, ws in rows:
        r.checks.append(Check("book", "crossed book (bid >= ask)", fam, cs, int(cn),
                              f"<= {th['crossed_seconds_warn']:.0f} s / {th['crossed_seconds_fail']:.0f} s",
                              grade_lo(cs, th["crossed_seconds_warn"], th["crossed_seconds_fail"]), "s",
                              f"{int(cn):,} rows, {100 * cs / secs if secs else 0:.3f} % of window time"))
        esh = es / secs if secs else None
        # Why info only: a far-out-of-the-money ladder strike has no bid for hours
        # and a BTC 15-minute window has no quotes before its start; both are
        # market structure, not recorder faults. The graded version below looks
        # only at live 15-minute windows, where a one-sided book is suspicious.
        r.checks.append(Check("book", "one side empty (time share)", fam, esh, int(en), "info", PASS if esh is not None else SKIP,
                              "pct", f"{int(en):,} rows, {es:.0f} s; structural for far strikes / unstarted windows"))
        wsh = ws / secs if secs else None
        r.checks.append(Check("book", f"spread > {so:.2f} (time share)", fam, wsh, int(wn),
                              f"<= {th['spread_outlier_time_warn']:.0%}",
                              grade_lo(wsh, th["spread_outlier_time_warn"], 1.0), "pct", f"{int(wn):,} rows, {ws:.0f} s"))
    hist = _df(con, "select * from _spread_hist")
    sp_rows = []
    for fam in sorted(hist["family"].unique().to_list()):
        h = hist.filter(pl.col("family") == fam)
        sp_rows.append((fam, int(h["n"].sum()), wquantile(h, "sp", "n", 0.5), wquantile(h, "sp", "n", 0.99), float(h["sp"].max())))
    r.tables["spread"] = pl.DataFrame(sp_rows, schema={"family": pl.String, "n_valid_rows": pl.Int64, "spread_p50": pl.Float64,
                                                       "spread_p99": pl.Float64, "spread_max": pl.Float64}, orient="row")
    worst = _df(con, """
        select asset_id, sum(crossed_ns) / 1e9 as crossed_s, sum(crossed_n) as n, min(first_crossed_ts) as first_ts
        from _tob_agg where crossed_n > 0 group by 1 order by 2 desc limit 5""")
    if worst.height:
        r.tables["crossed_worst"] = worst.with_columns(pl.col("asset_id").str.slice(0, 12),
                                                       pl.col("first_ts").map_elements(_fmt_ts, return_dtype=pl.String),
                                                       pl.col("crossed_s").round(1))
    live = _df(con, """
        select sum(live_n) as n, sum(live_ns) / 1e9 as secs, sum(live_empty_ns) / 1e9 as empty_s, sum(live_crossed_ns) / 1e9 as crossed_s,
               count(distinct asset_id) filter (where live_n > 0) as n_windows from _tob_agg""").row(0)
    n, secs, es, cs, nw = live
    if n:
        esh = es / secs if secs else None
        r.checks.append(Check("book", f"live 15 m window (first {900 - tail:.0f} s): one side empty (time share)", "btc_15m", esh, int(n),
                              f"<= {th['empty_side_time_warn']:.0%} / {th['empty_side_time_fail']:.0%}",
                              grade_lo(esh, th["empty_side_time_warn"], th["empty_side_time_fail"]), "pct",
                              f"{es:.0f} s of {secs:.0f} s in {int(nw)} live Up-token windows; crossed {cs:.1f} s"))
    return r


def a1_start(con: duckdb.DuckDBPyConnection, t0: int, t1: int) -> int | None:
    """First instant the recorder dropped a mirrored outcome-1 row (decision A1),
    from the health counters; None if the day never ran with A1.

    Why health and not a per-hour row ratio: the switch happened mid-hour on
    2026-11-07; the counter gives the exact instant, the ratio only the hour."""
    if "health" not in _views(con):
        return None
    try:
        # Why sql().fetchall(): execute().fetchone() leaves an unfinished result on
        # the connection and the next query raises ResourceInUse.
        rows = con.sql("select min(ts_ns) from health where mirror_dropped > 0").fetchall()
    except duckdb.BinderException:  # column absent on days before A1 existed
        return None
    return int(rows[0][0]) if rows and rows[0][0] is not None else None


def check_mirror(con: duckdb.DuckDBPyConnection, t0: int, t1: int, th: dict = THRESHOLDS) -> Result:
    """YES/NO consistency. Two eras:

    Pre-A1 (both tokens stored): on the 1-second depth grid, share of instants
    where YES best bid == 1 - NO best ask and YES best ask == 1 - NO best bid,
    and additionally the level-1 sizes match. Polymarket's book is unified, so
    this should be ~100 %; anything else is either a server-side asymmetry or a
    recorder/replay bug on one token.

    A1 era (outcome 1 stored only when a message is not an exact mirror): the
    rebuilt NO book is a mirror by construction, so the grid test is a tautology.
    Instead the kept raw outcome-1 delta rows are classified against the
    outcome-0 rows of the same frame: `exact_mirror_kept` (this row mirrored, but
    another row of the message did not), `size_differs`, `no_mirror_row`."""
    r = Result()
    views = _views(con)
    a1 = a1_start(con, t0, t1)
    pre_end = t1 if a1 is None else max(min(a1, t1), t0)
    if "depth_1s" in views and pre_end > t0:
        df = _df(con, f"""
            with d as (select d.ts_recv_ns as ts, a.condition_id as cond, a.outcome_index as oi, a.family,
                              d.bid_px_1, d.bid_cum_1, d.ask_px_1, d.ask_cum_1
                       from depth_1s d join assets a using (asset_id)
                       where d.valid and d.ts_recv_ns >= {t0} and d.ts_recv_ns < {pre_end})
            select y.family, count(*) as n,
                   count(*) filter (where abs(y.bid_px_1 + n.ask_px_1 - 1) < 1e-9 and abs(y.ask_px_1 + n.bid_px_1 - 1) < 1e-9) as px_ok,
                   count(*) filter (where abs(y.bid_px_1 + n.ask_px_1 - 1) < 1e-9 and abs(y.ask_px_1 + n.bid_px_1 - 1) < 1e-9
                                      and y.bid_cum_1 = n.ask_cum_1 and y.ask_cum_1 = n.bid_cum_1) as full_ok
            from d y join d n on y.cond = n.cond and y.ts = n.ts
            where y.oi = 0 and n.oi = 1 group by 1 order by 1""")
        tot = df.select(pl.col("n").sum(), pl.col("px_ok").sum(), pl.col("full_ok").sum()).row(0)
        for fam, n, px, full in [("all", *tot)] + [tuple(x) for x in df.iter_rows()]:
            r.checks.append(Check("mirror", "pre-A1: YES/NO prices mirrored (1 s grid)", fam, px / n if n else None, int(n),
                                  f">= {th['mirror_px_pass']:.1%} / {th['mirror_px_warn']:.0%}",
                                  grade_hi(px / n if n else None, th["mirror_px_pass"], th["mirror_px_warn"]), "pct",
                                  f"until {_fmt_ts(pre_end)} UTC"))
            r.checks.append(Check("mirror", "pre-A1: prices and level-1 sizes mirrored", fam, full / n if n else None, int(n),
                                  "info", PASS if n else SKIP, "pct"))
        if df.height == 0:
            r.checks.append(Check("mirror", "pre-A1: YES/NO prices mirrored (1 s grid)", "all", None, 0, "", SKIP, "pct",
                                  "no instants with both tokens valid"))
    if a1 is not None and a1 < t1 and "book_events" in views:
        a1s = max(a1, t0)
        kept = _df(con, f"""
            with d as (select b.ts_recv_ns as ts, a.condition_id as cond, a.outcome_index as oi, a.family, b.side, b.price, b.size
                       from book_events b join assets a using (asset_id)
                       where b.event_type = 'delta' and b.ts_recv_ns >= {a1s}),
            o1 as (select * from d where oi = 1), o0 as (select * from d where oi = 0),
            m as (select o1.family, o1.cond, o1.ts, o1.side, o1.price, o1.size,
                         count(o0.size) as n_sib, bool_or(o0.size = o1.size) as exact
                  from o1 left join o0 on o0.cond = o1.cond and o0.ts = o1.ts and o0.side <> o1.side
                                       and abs(o0.price + o1.price - 1) < 1e-9
                  group by all)
            select family,
                   case when n_sib = 0 then 'no_mirror_row' when exact then 'exact_mirror_kept' else 'size_differs' end as reason,
                   count(*) as n from m group by 1, 2 order by 1, 2""")
        base = _df(con, f"""
            select a.family, count(*) filter (where a.outcome_index = 0 and b.event_type = 'delta') as o0_deltas,
                   count(distinct b.snapshot_id) filter (where a.outcome_index = 1 and b.event_type in ('snapshot_ws', 'snapshot_rest')) as o1_snapshots
            from book_events b join assets a using (asset_id) where b.ts_recv_ns >= {a1s} group by 1 order by 1""")
        r.tables["mirror_kept"] = kept
        fams = sorted(set(base["family"].to_list()) | set(kept["family"].to_list()))
        for fam in ["all"] + fams:
            k = kept if fam == "all" else kept.filter(pl.col("family") == fam)
            b = base if fam == "all" else base.filter(pl.col("family") == fam)
            n_kept = int(k["n"].sum()) if k.height else 0
            o0 = int(b["o0_deltas"].sum()) if b.height else 0
            snaps = int(b["o1_snapshots"].sum()) if b.height else 0
            share = n_kept / o0 if o0 else None
            by = ", ".join(f"{reason}={int(n)}" for reason, n in k.group_by("reason").agg(pl.col("n").sum()).sort("reason").iter_rows())
            r.checks.append(Check("mirror", "A1: kept outcome-1 deltas / outcome-0 deltas", fam, share, o0,
                                  f"<= {th['mirror_kept_share_warn']:.0%}", grade_lo(share, th["mirror_kept_share_warn"], 1.0),
                                  "pct", f"from {_fmt_ts(a1s)} UTC; kept {n_kept:,} ({by or 'none'}); {snaps} outcome-1 snapshots kept"))
    if a1 is None and "depth_1s" not in views:
        r.checks.append(Check("mirror", "YES/NO mirror", "all", None, 0, "", SKIP, "pct", "no depth_1s table"))
    return r


def check_trades(con: duckdb.DuckDBPyConnection, th: dict = THRESHOLDS) -> Result:
    """Trades against the book they hit.

    Each trade is first tied to its fill delta: the nearest size decrease (within
    +-1 s) at the trade's price on the side it consumed, on its own token (taker
    BUY consumes the SELL side at p) or on the sibling (the same level stored as
    the sibling's BUY side at 1-p). Why not the labeller's `fill` rows: the
    labeller names one trade per delta, but one taker order that hits several
    makers at one level produces several trade messages and ONE delta, so the
    other trades would look unmatched; and it tags every size decrease at that
    price within 1 s with the trade, so the first tagged row is often a cancel.

    Reference book = the replayed top of book just BEFORE the fill delta
    (`least(trade ts, delta ts) - 1 ns`), or as of the trade's receive time when
    no delta was found. Why: the delta that removes the filled level usually lands
    before the trade message; judged against the post-fill book a taker BUY would
    look like it traded below the ask. Only trades whose reference instant lies
    inside a loose clean window and whose book is valid are judged."""
    r = Result()
    views = _views(con)
    if "trades" not in views or "tob" not in views:
        r.checks.append(Check("trades", "trades vs book", "all", None, 0, "", SKIP, "pct", "no trades or tob table"))
        return r
    has_el = "events_labeled" in views
    lw = NS  # +-1 s, the labeller's window
    if has_el:
        con.execute("""
            create or replace view _down as
            select ts_recv_ns, asset_id, side, cast(round(price * 1e6) as bigint) as px, size_change
            from events_labeled where size_change < 0""")
        # one key row per trade for its own token and one for the sibling
        keys_sql = f"""
        keys as (
            select ts, asset_id as tid, asset_id as key_asset, case when side = 'BUY' then 'SELL' else 'BUY' end as key_side,
                   cast(round(price * 1e6) as bigint) as px from tr
            union all
            select ts, asset_id, sibling, side, cast(round((1 - price) * 1e6) as bigint) from tr where sibling is not null),
        near as (
            select k.ts, k.tid,
                   case when b.ts_recv_ns is null then a.ts_recv_ns when a.ts_recv_ns is null then b.ts_recv_ns
                        when k.ts - b.ts_recv_ns <= a.ts_recv_ns - k.ts then b.ts_recv_ns else a.ts_recv_ns end as dts
            from keys k
            asof left join _down b on k.key_asset = b.asset_id and k.key_side = b.side and k.px = b.px and k.ts >= b.ts_recv_ns
            asof left join _down a on k.key_asset = a.asset_id and k.key_side = a.side and k.px = a.px and k.ts < a.ts_recv_ns),
        fills as (select ts, tid as asset_id, arg_min(dts, abs(dts - ts)) as dts from near where abs(dts - ts) <= {lw} group by 1, 2),"""
    else:
        keys_sql = "fills as (select null::bigint as ts, ''::varchar as asset_id, null::bigint as dts where false),"
    df = _df(con, f"""
        with tr as (select t.ts_recv_ns as ts, t.asset_id, t.price, t.size, t.side, coalesce(a.family, 'unknown') as family, a.sibling
                    from trades t left join assets a using (asset_id)),
        {keys_sql}
        m as (select tr.*, f.dts from tr left join fills f on f.ts = tr.ts and f.asset_id = tr.asset_id),
        ref as (select *, coalesce(least(ts, dts) - 1, ts) as tref from m),
        j as (select r.*, b.best_bid, b.best_ask, b.valid as book_valid, w.t_end
              from ref r
              asof left join tob b on r.asset_id = b.asset_id and r.tref >= b.ts_recv_ns
              asof left join cw w on r.asset_id = w.asset_id and r.tref >= w.t_start),
        g as (select *, (t_end is not null and tref < t_end and book_valid) as judged from j)
        select family, count(*) as n_trades, count(*) filter (where judged) as n_judged,
               count(*) filter (where judged and price >= best_bid and price <= best_ask) as in_spread,
               count(*) filter (where judged and ((side = 'BUY' and price >= best_ask) or (side = 'SELL' and price <= best_bid))) as side_ok,
               count(*) filter (where judged and ((side = 'BUY' and price < best_bid) or (side = 'SELL' and price > best_ask))) as wrong_side,
               count(*) filter (where judged and ((side = 'BUY' and price > best_ask) or (side = 'SELL' and price < best_bid))) as sweep,
               count(*) filter (where t_end is not null and tref < t_end) as in_window,
               count(*) filter (where t_end is not null and tref < t_end and dts is not null) as matched,
               count(*) filter (where side = 'BUY') as n_buy
        from g group by 1 order by 1""")
    tot = df.select([pl.col(c).sum() for c in df.columns[1:]]).row(0)
    for fam, n, nj, insp, sok, wrong, sweep, inw, matched, nbuy in [("all", *tot)] + [tuple(x) for x in df.iter_rows()]:
        v = insp / nj if nj else None
        r.checks.append(Check("trades", "price inside/at the spread (pre-trade book)", fam, v, int(nj),
                              f">= {th['trade_in_spread_pass']:.0%} / {th['trade_in_spread_warn']:.0%}",
                              grade_hi(v, th["trade_in_spread_pass"], th["trade_in_spread_warn"]), "pct",
                              f"{int(n):,} trades, {int(n - nj):,} not judged (outside windows / book not valid); {int(nbuy):,} taker BUY; "
                              f"{100 * sweep / nj if nj else 0:.1f} % beyond the touch = multi-level sweeps"))
        v = sok / nj if nj else None
        r.checks.append(Check("trades", "side consistent (BUY >= ask, SELL <= bid)", fam, v, int(nj),
                              f">= {th['trade_side_pass']:.0%} / {th['trade_side_warn']:.0%}",
                              grade_hi(v, th["trade_side_pass"], th["trade_side_warn"]), "pct"))
        v = wrong / nj if nj else None
        r.checks.append(Check("trades", "wrong side (BUY < bid or SELL > ask)", fam, v, int(nj),
                              f"<= {th['trade_wrong_side_warn']:.1%} / {th['trade_wrong_side_fail']:.0%}",
                              grade_lo(v, th["trade_wrong_side_warn"], th["trade_wrong_side_fail"]), "pct", f"{int(wrong):,} trades"))
        if has_el:
            v = matched / inw if inw else None
            r.checks.append(Check("trades", "trade has a size decrease at its price within 1 s", fam, v, int(inw),
                                  f">= {th['trade_matched_pass']:.0%} / {th['trade_matched_warn']:.0%}",
                                  grade_hi(v, th["trade_matched_pass"], th["trade_matched_warn"]), "pct",
                                  f"{int(inw - matched):,} unmatched of {int(inw):,} in windows"))
    if has_el:
        lab = _df(con, """
            select coalesce(a.family, 'unknown') as family,
                   count(*) filter (where size_change < 0) as n_down,
                   count(*) filter (where label = 'fill') as n_fill,
                   count(distinct (e.trade_ts_recv_ns, e.asset_id)) filter (where label = 'fill') as n_trades,
                   count(*) filter (where label = 'fill' and abs(e.size_change + e.trade_size) < 1e-6) as exact_size
            from events_labeled e left join assets a using (asset_id) group by 1 order by 1""")
        t2 = lab.select(pl.col("n_down").sum(), pl.col("n_fill").sum(), pl.col("n_trades").sum(), pl.col("exact_size").sum()).row(0)
        for fam, nd, nf, nt, ex in [("all", *t2)] + [tuple(x) for x in lab.iter_rows()]:
            # Why this check: a trade should explain ONE delta (the level it consumed),
            # so fill rows / distinct matched trades should be ~1. The labeller's
            # nearest-trade rule is one-directional (delta -> trade), so every cancel
            # at the same price within 1 s of a trade inherits its label.
            ratio = nf / nt if nt else None
            r.checks.append(Check("trades", "fill rows per labelled trade (replay over-labelling)", fam, ratio, int(nf),
                                  "<= 1.5 / 3", grade_lo(ratio, 1.5, 3.0), "float",
                                  f"{int(nt):,} trades label {int(nf):,} fill rows = {100 * nf / nd if nd else 0:.1f} % of {int(nd):,} size "
                                  f"decreases; {100 * ex / nf if nf else 0:.1f} % of fill rows shrink by exactly the trade size"))
    excl = int(tot[0] - tot[1])
    if excl:
        r.excluded.append(f"{excl:,} of {int(tot[0]):,} trades not judged against the book (reference instant outside a clean window, or book not valid)")
    return r


def check_latency(con: duckdb.DuckDBPyConnection, th: dict = THRESHOLDS) -> Result:
    """recv - exchange timestamp on deltas: level, tails, negatives, drift.

    One SQL pass builds a histogram per (minute, family) at 0.1 ms resolution;
    quantiles, per-hour tables and the drift fit are computed from it in polars.
    Drift is the least-squares slope of the per-minute 5th percentile against
    time. Why p5 and not the median: the floor of the latency distribution is
    clock offset + network minimum; the median moves with load (busy minutes are
    slower) and would show the day's activity profile, not clock drift."""
    r = Result()
    if "book_events" not in _views(con):
        r.checks.append(Check("latency", "delta latency", "all", None, 0, "", SKIP, "ms", "no book_events"))
        return r
    hist = _df(con, f"""
        select b.ts_recv_ns // {60 * NS} as minute, coalesce(a.family, 'unknown') as family,
               round(b.ts_recv_ns / 1e6 - b.ts_exch_ms, 1) as lat, count(*) as n
        from book_events b left join assets a using (asset_id)
        where b.event_type = 'delta' and b.ts_exch_ms > 0 group by all""")
    if hist.height == 0:
        r.checks.append(Check("latency", "delta latency", "all", None, 0, "", SKIP, "ms", "no deltas"))
        return r
    fams = ["all"] + sorted(hist["family"].unique().to_list())
    for fam in fams:
        h = hist if fam == "all" else hist.filter(pl.col("family") == fam)
        n = int(h["n"].sum())
        p50, p95, p99 = (wquantile(h, "lat", "n", q) for q in (0.5, 0.95, 0.99))
        neg = int(h.filter(pl.col("lat") < 0)["n"].sum())
        lmin = float(h["lat"].min())
        r.checks.append(Check("latency", "delta recv - exch p50", fam, p50, n,
                              f"<= {th['latency_p50_warn_ms']:.0f} / {th['latency_p50_fail_ms']:.0f} ms",
                              grade_lo(p50, th["latency_p50_warn_ms"], th["latency_p50_fail_ms"]), "ms",
                              f"p95 {p95:.0f} ms, p99 {p99:.0f} ms"))
        r.checks.append(Check("latency", "delta recv - exch p99", fam, p99, n, f"<= {th['latency_p99_warn_ms']:.0f} ms",
                              grade_lo(p99, th["latency_p99_warn_ms"], float("inf")), "ms"))
        share = neg / n if n else None
        r.checks.append(Check("latency", "negative latency (recv before exch)", fam, share, n,
                              f"<= {th['latency_negative_share_warn']:.0%} / {th['latency_negative_share_fail']:.0%}",
                              grade_lo(share, th["latency_negative_share_warn"], th["latency_negative_share_fail"]), "pct",
                              f"{neg:,} rows; min {lmin:.1f} ms"))
    per_min = hist.group_by("minute").agg(pl.struct("lat", "n").alias("h")).sort("minute")
    p5 = [wquantile(pl.DataFrame({"lat": [d["lat"] for d in hh], "n": [d["n"] for d in hh]}), "lat", "n", 0.05) for hh in per_min["h"]]
    x = (per_min["minute"].cast(pl.Float64) / 60.0)
    slope, r2 = _slope(x, pl.Series(p5, dtype=pl.Float64))
    r.checks.append(Check("latency", "clock drift (slope of per-minute p5)", "all", slope, per_min.height,
                          f"|slope| <= {th['latency_drift_warn_ms_per_h']:.1f} ms/h",
                          SKIP if slope is None else (PASS if abs(slope) <= th["latency_drift_warn_ms_per_h"] else WARN), "float",
                          f"ms/h, r² {r2:.2f}" if r2 is not None else "fewer than 3 minutes"))
    hrows = []
    for hour, h in hist.with_columns((pl.col("minute") // 60 * 3600).alias("hour")).group_by("hour", maintain_order=True):
        hour = hour[0]
        h = h.group_by("lat").agg(pl.col("n").sum())
        hrows.append((_fmt_hour(hour), int(h["n"].sum()), wquantile(h, "lat", "n", 0.05), wquantile(h, "lat", "n", 0.5),
                      wquantile(h, "lat", "n", 0.95), wquantile(h, "lat", "n", 0.99), int(h.filter(pl.col("lat") < 0)["n"].sum())))
    r.tables["latency_per_hour"] = pl.DataFrame(sorted(hrows), schema={"hour_utc": pl.String, "n": pl.Int64, "p5": pl.Float64, "p50": pl.Float64,
                                                                      "p95": pl.Float64, "p99": pl.Float64, "neg": pl.Int64}, orient="row")
    if "trades" in _views(con):
        th_ = _df(con, "select round(ts_recv_ns / 1e6 - ts_exch_ms, 1) as lat, count(*) as n from trades where ts_exch_ms > 0 group by 1")
        n = int(th_["n"].sum()) if th_.height else 0
        p50, p99 = wquantile(th_, "lat", "n", 0.5), wquantile(th_, "lat", "n", 0.99)
        r.checks.append(Check("latency", "trade recv - exch p50", "all", p50, n, "info", PASS if n else SKIP, "ms",
                              f"p99 {p99:.0f} ms" if p99 is not None else ""))
    return r


def check_spot(con: duckdb.DuckDBPyConnection, th: dict = THRESHOLDS) -> Result:
    """Binance feed integrity: update_id monotonic, trade_id holes, latency,
    reconnect markers, quote gaps, and (if BTC 15-minute markets overlap) the
    direction sanity between Binance and the Up token."""
    r = Result()
    views = _views(con)
    if "spot_quotes" not in views and "spot_trades" not in views:
        r.checks.append(Check("spot", "Binance feed", "all", None, 0, "", SKIP, "int", "no spot tables in this dataset"))
        return r
    gap = th["spot_gap_s"]
    if "spot_quotes" in views:
        q = _df(con, f"""
            with s as (select ts_recv_ns, symbol, update_id, lag(update_id) over (partition by symbol order by ts_recv_ns) as prev_u,
                              lag(ts_recv_ns) over (order by ts_recv_ns) as prev_ts
                       from spot_quotes)
            select count(*) filter (where symbol <> '') as n_quotes,
                   count(*) filter (where symbol <> '' and prev_u is not null and update_id <= prev_u) as non_monotonic,
                   count(*) filter (where symbol = '') as reconnect_markers,
                   count(*) filter (where prev_ts is not null and ts_recv_ns - prev_ts > {gap * NS}) as n_gaps,
                   coalesce(max(ts_recv_ns - prev_ts), 0) / 1e9 as max_gap_s,
                   min(ts_recv_ns) as first_ts, max(ts_recv_ns) as last_ts
            from s""").row(0)
        nq, nonmono, markers, ngaps, maxgap, fts, lts = q
        r.checks.append(Check("spot", "quote update_id monotonic (violations)", "all", nonmono, int(nq), "= 0",
                              PASS if nonmono == 0 else FAIL, "int",
                              f"{_fmt_ts(fts)}–{_fmt_ts(lts)} UTC" if fts else "no quotes"))
        r.checks.append(Check("spot", "feed reconnect markers", "all", markers, int(nq), "info", PASS if markers == 0 else WARN, "int"))
        r.checks.append(Check("spot", f"quote gaps > {gap:.0f} s", "all", ngaps, int(nq), "= 0", PASS if ngaps == 0 else WARN, "int",
                              f"max gap {maxgap:.1f} s"))
        if ngaps:
            gaps = _df(con, f"""
                with s as (select ts_recv_ns, lag(ts_recv_ns) over (order by ts_recv_ns) as prev_ts from spot_quotes)
                select prev_ts, ts_recv_ns, (ts_recv_ns - prev_ts) / 1e9 as gap_s from s
                where ts_recv_ns - prev_ts > {gap * NS} order by 3 desc limit 20""")
            r.tables["spot_gaps"] = gaps.with_columns(pl.col("prev_ts").map_elements(_fmt_ts, return_dtype=pl.String).alias("from_utc"),
                                                      pl.col("ts_recv_ns").map_elements(_fmt_ts, return_dtype=pl.String).alias("to_utc"),
                                                      pl.col("gap_s").round(1)).select("from_utc", "to_utc", "gap_s")
            for a, b, g in gaps.iter_rows():
                r.excluded.append(f"spot quotes gap {_fmt_ts(a)}–{_fmt_ts(b)} UTC ({g:.1f} s)")
    if "spot_trades" in views:
        t = _df(con, """
            with s as (select ts_recv_ns, symbol, trade_id, event_ms, ts_exch_ms,
                              lag(trade_id) over (partition by symbol order by trade_id) as prev_id from spot_trades)
            select count(*) as n, count(*) filter (where prev_id is not null and trade_id - prev_id > 1) as n_holes,
                   coalesce(sum(trade_id - prev_id - 1) filter (where prev_id is not null and trade_id - prev_id > 1), 0) as missing_ids,
                   count(*) filter (where prev_id is not null and trade_id = prev_id) as duplicates,
                   quantile_cont(ts_recv_ns / 1e6 - event_ms, 0.5) as p50, quantile_cont(ts_recv_ns / 1e6 - event_ms, 0.99) as p99,
                   quantile_cont(event_ms - ts_exch_ms, 0.5) as emit_p50
            from s""").row(0)
        n, nholes, missing, dups, p50, p99, emit = t
        r.checks.append(Check("spot", "trade_id holes", "all", nholes, int(n), "= 0", PASS if nholes == 0 else WARN, "int",
                              f"{int(missing):,} ids missing; {int(dups)} duplicates"))
        r.checks.append(Check("spot", "trade recv - event_ms p50", "all", p50, int(n), f"p99 <= {th['spot_latency_p99_warn_ms']:.0f} ms",
                              grade_lo(p99, th["spot_latency_p99_warn_ms"], float("inf")), "ms",
                              f"p99 {p99:.0f} ms; Binance emit - trade time p50 {emit:.0f} ms" if p99 is not None else ""))
        if nholes:
            holes = _df(con, """
                with s as (select ts_recv_ns, trade_id, lag(trade_id) over (partition by symbol order by trade_id) as prev_id from spot_trades)
                select ts_recv_ns, prev_id, trade_id, trade_id - prev_id - 1 as missing from s
                where prev_id is not null and trade_id - prev_id > 1 order by 4 desc limit 20""")
            r.tables["spot_holes"] = holes.with_columns(pl.col("ts_recv_ns").map_elements(_fmt_ts, return_dtype=pl.String).alias("at_utc")) \
                .select("at_utc", "prev_id", "trade_id", "missing")
    if "spot_quotes" in views and "depth_1s" in views:
        r.extend(_spot_direction(con, th))
    return r


def _btc15_windows(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Up-token asset_id and [start, end) of every BTC 15-minute market, from the
    slug `btc-updown-15m-<unix start>`; the window is 900 s long."""
    df = _df(con, "select asset_id, market_slug from assets where family = 'btc_15m' and outcome_index = 0 and market_slug like 'btc-updown-15m-%'")
    if not df.height:
        return df
    return df.with_columns(pl.col("market_slug").str.extract(r"(\d+)$").cast(pl.Int64).alias("start_s")) \
        .drop_nulls("start_s").with_columns((pl.col("start_s") + 900).alias("end_s"))


def _register_btc15(con: duckdb.DuckDBPyConnection) -> bool:
    """Register the `_btc15` view (Up token, start_s, end_s) once, empty if the
    dataset has no BTC 15-minute markets; returns whether it has rows."""
    if "_btc15" in _views(con):
        return bool(con.sql("select count(*) from _btc15").fetchall()[0][0])
    win = _btc15_windows(con)
    if not win.height:
        win = pl.DataFrame(schema={"asset_id": pl.String, "market_slug": pl.String, "start_s": pl.Int64, "end_s": pl.Int64})
    register_frames(con, _btc15=win)
    return bool(win.height)


def _binance_mid_1s(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Last Binance mid of every second (as-of: quotes with ts <= second)."""
    return _df(con, f"""
        select ((ts_recv_ns - 1) // {NS} + 1) as sec, arg_max((bid + ask) / 2, ts_recv_ns) as bmid
        from spot_quotes where symbol <> '' and bid is not null group by 1 order by 1""")


def _poly_live_grid(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """1 s grid of the LIVE BTC 15-minute Up token: (sec, slug, start_s, mid).
    Why only the live window: the next two windows are subscribed early but their
    strike is not set yet, so their price does not follow BTC."""
    if not _register_btc15(con):
        return pl.DataFrame(schema={"sec": pl.Int64, "slug": pl.String, "start_s": pl.Int64, "mid": pl.Float64})
    return _df(con, f"""
        select d.ts_recv_ns // {NS} as sec, w.market_slug as slug, w.start_s, (d.bid_px_1 + d.ask_px_1) / 2 as mid
        from depth_1s d join _btc15 w using (asset_id)
        where d.valid and d.ts_recv_ns >= w.start_s * {NS} and d.ts_recv_ns < w.end_s * {NS} order by 1""")


def _spot_direction(con: duckdb.DuckDBPyConnection, th: dict) -> Result:
    """Sanity: when Binance is above the window's open, the Up token should trade
    above 0.5 (and vice versa). Open = first Binance mid at/after the window start
    (the real reference is a Chainlink price; this is a sanity check, not a model)."""
    r = Result()
    poly = _poly_live_grid(con)
    bz = _binance_mid_1s(con)
    if not poly.height or not bz.height:
        r.checks.append(Check("spot", "Binance direction vs Up token", "btc_15m", None, 0, "", SKIP, "pct", "no overlap of spot and live 15 m windows"))
        return r
    opens = bz.join(poly.select("slug", "start_s").unique(), how="cross").filter(pl.col("sec") >= pl.col("start_s")) \
        .group_by("slug").agg(pl.col("bmid").sort_by("sec").first().alias("open"), pl.col("sec").min().alias("open_sec"),
                              pl.col("start_s").first())
    opens = opens.filter(pl.col("open_sec") - pl.col("start_s") <= 60)  # the feed must have been up at the open
    # Why 0.1 / 0.05 %: the market's reference open is a Chainlink price, which
    # sits a few basis points away from the Binance mid; only moves clearly larger
    # than that, with a token price clearly away from 0.5, carry a direction.
    g = poly.join(opens, on="slug", how="inner").join(bz, on="sec", how="inner") \
        .filter(((pl.col("mid") - 0.5).abs() >= 0.10) & ((pl.col("bmid") / pl.col("open") - 1).abs() >= 5e-4))
    n = g.height
    agree = int(((g["bmid"] > g["open"]) == (g["mid"] > 0.5)).sum()) if n else 0
    v = agree / n if n else None
    nwin = opens.height
    status = grade_hi(v, th["spot_direction_pass"], 0.5)
    if nwin < 3 and status != SKIP:
        status = PASS if v is not None and v >= th["spot_direction_pass"] else WARN  # too few windows to FAIL on
    r.checks.append(Check("spot", "Binance direction vs Up token (|mid-0.5| >= 0.1, |move| >= 0.05 %)", "btc_15m", v, n,
                          f">= {th['spot_direction_pass']:.0%} (info below 3 windows)", status, "pct",
                          f"{nwin} windows with a Binance open"))
    return r


def xcorr_1s(poly: pl.DataFrame, bz: pl.DataFrame, max_lag: int = 30) -> pl.DataFrame:
    """Cross-correlation of 1 s returns: Binance log-mid change at t vs Polymarket
    mid change at t + lag, pooled over markets. Positive lag = Binance leads.

    Why diffs and not levels: both series are random walks; levels correlate at
    every lag. Why pooled: one 15-minute window has ~900 points and a discrete
    (1 c) Polymarket price; pooling across windows is what makes the peak visible."""
    g = poly.join(bz, on="sec", how="inner").sort(["slug", "sec"])
    g = g.with_columns(
        (pl.col("mid") - pl.col("mid").shift(1)).over("slug").alias("rp"),
        (pl.col("bmid").log() - pl.col("bmid").log().shift(1)).over("slug").alias("rb"),
        (pl.col("sec") - pl.col("sec").shift(1)).over("slug").alias("gap"),
    ).filter(pl.col("gap") == 1).drop_nulls(["rp", "rb"])
    rows = []
    for lag in range(-max_lag, max_lag + 1):
        # rp shifted by -lag: rp at t+lag sits next to rb at t
        s = g.with_columns(pl.col("rp").shift(-lag).over("slug").alias("rp_l")).drop_nulls("rp_l")
        c = s.select(pl.corr("rb", "rp_l")).item() if s.height > 2 else None
        rows.append((lag, c, s.height))
    return pl.DataFrame(rows, schema={"lag_s": pl.Int64, "corr": pl.Float64, "n": pl.Int64}, orient="row")


def check_cross_feed(con: duckdb.DuckDBPyConnection, th: dict = THRESHOLDS) -> Result:
    """Timestamp sanity across feeds: Binance must lead (or be simultaneous with)
    the Polymarket BTC 15-minute mid; if Polymarket led, our clocks or receive
    paths would be wrong. Reports the lag of maximum correlation."""
    r = Result()
    views = _views(con)
    if "spot_quotes" not in views or "depth_1s" not in views:
        r.checks.append(Check("xfeed", "Binance vs Polymarket lead (1 s xcorr)", "btc_15m", None, 0, "", SKIP, "lag", "needs spot_quotes and depth_1s"))
        return r
    poly = _poly_live_grid(con)
    bz = _binance_mid_1s(con)
    if not poly.height or not bz.height:
        r.checks.append(Check("xfeed", "Binance vs Polymarket lead (1 s xcorr)", "btc_15m", None, 0, "", SKIP, "lag", "no overlap of spot and live 15 m windows"))
        return r
    xc = xcorr_1s(poly, bz)
    valid = xc.drop_nulls("corr")
    if not valid.height:
        r.checks.append(Check("xfeed", "Binance vs Polymarket lead (1 s xcorr)", "btc_15m", None, 0, "", SKIP, "lag", "too few overlapping seconds"))
        return r
    best = valid.sort("corr", descending=True).row(0)
    lag, c, n = best
    zero = valid.filter(pl.col("lag_s") == 0)["corr"].item() if valid.filter(pl.col("lag_s") == 0).height else None
    lo, hi = th["xcorr_lag_pass_min_s"], th["xcorr_lag_pass_max_s"]
    if c < th["xcorr_min_corr"]:
        status, note = WARN, f"max corr {c:.3f} below {th['xcorr_min_corr']}: no usable signal in {n} s"
    else:
        status = PASS if lo <= lag <= hi else FAIL
        note = f"max corr {c:.3f} at lag {lag:+d} s (corr at 0: {zero:.3f}); {n:,} overlapping seconds, {poly['slug'].n_unique()} windows"
    r.checks.append(Check("xfeed", "Binance vs Polymarket lead (1 s xcorr)", "btc_15m", float(lag), int(n),
                          f"lag in [{lo}, {hi}] s, corr >= {th['xcorr_min_corr']}", status, "lag", note))
    r.tables["xcorr"] = xc.filter(pl.col("lag_s").abs() <= 10).with_columns(pl.col("corr").round(4))
    return r


# ---- strict windows ---------------------------------------------------------------------


def strict_windows(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """clean_windows trimmed to [t_start, last_verified], windows never verified
    dropped, plus each asset's strict coverage share of its subscribed time.

    Why trim to last_verified: replay knows a window went bad only when the next
    snapshot disagrees; the dropped delta may sit anywhere after the last
    snapshot that agreed. Everything after that point is unproven."""
    return _df(con, """
        with w as (select w.asset_id, coalesce(a.family, 'unknown') as family, w.t_start, w.last_verified as t_end,
                     w.start_kind, w.end_reason, w.n_events, (w.own_verified is not null) as verified_own,
                     w.t_end as t_end_loose
              from cw w left join assets a using (asset_id) where w.last_verified is not null and w.last_verified > w.t_start),
        share as (select w.asset_id, sum(t_end - t_start) * 1.0 / nullif(max(e - s), 0) as strict_share
                  from w join span using (asset_id) group by 1)
        select w.*, round(coalesce(share.strict_share, 0), 4) as asset_strict_share
        from w left join share using (asset_id) order by family, asset_id, t_start""")


# ---- report -------------------------------------------------------------------------------


def _md_table(df: pl.DataFrame) -> str:
    cols = df.columns
    out = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for row in df.iter_rows():
        out.append("| " + " | ".join("" if v is None else (f"{v:,}" if isinstance(v, int) else str(v)) for v in row) + " |")
    return "\n".join(out)


def render_report(date: str, data_dir: Path, t0: int, t1: int, res: Result, timings: dict[str, float],
                  replay_note: str, th: dict = THRESHOLDS) -> str:
    counts = {s: sum(1 for c in res.checks if c.status == s) for s in (FAIL, WARN, PASS, SKIP)}
    lines = [f"# Data quality — {date}", "",
             f"Data root `{data_dir}`; window {datetime.fromtimestamp(t0 / 1e9, tz=timezone.utc):%Y-%m-%d %H:%M} → "
             f"{datetime.fromtimestamp(t1 / 1e9, tz=timezone.utc):%Y-%m-%d %H:%M} UTC. {replay_note} "
             f"Checks took {timings.get('checks', 0):.0f} s" + (f" ({', '.join(f'{k} {v:.0f} s' for k, v in timings.items() if k not in ('checks', 'replay'))})" if timings else "") + ".", "",
             f"**{counts[FAIL]} FAIL · {counts[WARN]} WARN · {counts[PASS]} PASS · {counts[SKIP]} SKIP**", "",
             "| section | check | family | value | n | threshold | status | note |", "|---|---|---|---|---|---|---|---|"]
    for c in res.checks:
        lines.append(f"| {c.section} | {c.name} | {c.family} | {c.value_str()} | {c.n:,} | {c.threshold} | **{c.status}** | {c.note} |")
    lines += ["", "## Excluded", ""]
    lines += [f"- {e}" for e in res.excluded] or ["- nothing"]
    lines += ["", "Strict clean windows (trimmed to the last verified instant) are in `clean_windows_strict.parquet`; "
              "analyses should read that file, not `clean_windows`.", "", "## Details", ""]
    titles = {"per_hour": "Raw book_events per hour", "end_reason": "Clean windows by end reason", "spread": "Spread inside clean windows (valid rows)",
              "crossed_worst": "Tokens with the most crossed time", "mirror_kept": "A1: kept outcome-1 delta rows by reason",
              "latency_per_hour": "Delta latency per hour (ms)", "spot_gaps": "Binance quote gaps", "spot_holes": "Binance trade_id holes",
              "xcorr": "Cross-correlation of 1 s returns (lag > 0: Binance leads)"}
    for key, title in titles.items():
        if key in res.tables and res.tables[key].height:
            lines += [f"### {title}", "", _md_table(res.tables[key]), ""]
    lines += ["## Thresholds", "", "```", json.dumps(th, indent=1), "```", ""]
    return "\n".join(lines)


# ---- driver ----------------------------------------------------------------------------------


@dataclass
class QualityConfig:
    date: str
    data_dir: Path
    out_dir: Path
    replay_out: Path | None = None  # default <out_dir>/replay
    t0: int = 0
    t1: int = 0
    thresholds: dict = field(default_factory=lambda: dict(THRESHOLDS))


def ensure_replay(cfg: QualityConfig) -> tuple[Path, str, float]:
    """Run `polymm replay` for the window unless `<replay_out>/derived/clean_windows`
    already exists. Returns (derived_dir, note, seconds)."""
    rp_out = cfg.replay_out or (cfg.out_dir / "replay")
    derived = rp_out / "derived"
    if (derived / "clean_windows").exists() and any((derived / "clean_windows").rglob("*.parquet")):
        return derived, f"Replay reused from `{rp_out}`.", 0.0
    log.info("replaying %s -> %s", cfg.date, rp_out)
    w0 = time.perf_counter()
    stats = Replayer(ReplayConfig(data_dir=cfg.data_dir, out_dir=rp_out, t0=cfg.t0, t1=cfg.t1)).run()
    secs = time.perf_counter() - w0
    return derived, (f"Replay ran in {secs:.0f} s ({stats['rows']:,} rows, {stats['reconnects']} reconnects, "
                     f"{stats['ws_mismatch']} WS mismatches, {stats['resyncs']} REST resyncs)."), secs


def run_quality(cfg: QualityConfig) -> Result:
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    derived, note, rp_s = ensure_replay(cfg)
    con = duckdb.connect()
    have = open_views(con, cfg.data_dir, derived, cfg.t0, cfg.t1, tmp_dir=cfg.out_dir / "tmp")
    res = Result()
    timings: dict[str, float] = {"replay": rp_s}
    th = cfg.thresholds
    steps = [("coverage", lambda: check_coverage(con, cfg.t0, cfg.t1, th)), ("book", lambda: check_book_sanity(con, th)),
             ("mirror", lambda: check_mirror(con, cfg.t0, cfg.t1, th)), ("trades", lambda: check_trades(con, th)),
             ("latency", lambda: check_latency(con, th)), ("spot", lambda: check_spot(con, th)),
             ("xfeed", lambda: check_cross_feed(con, th))]
    if "clean_windows" not in have or "tob" not in have:
        raise RuntimeError(f"replay output under {derived} has no clean_windows/tob tables")
    for name, fn in steps:
        s0 = time.perf_counter()
        try:
            res.extend(fn())
        except Exception as e:  # Why: one broken check must not hide the others' numbers
            log.exception("check %s failed", name)
            res.checks.append(Check(name, "check crashed", "all", None, 0, "", FAIL, "int", repr(e)[:200]))
        timings[name] = time.perf_counter() - s0
        log.info("check %s: %.1f s", name, timings[name])
    timings["checks"] = sum(v for k, v in timings.items() if k != "replay")
    sw = strict_windows(con)
    sw.write_parquet(cfg.out_dir / "clean_windows_strict.parquet")
    md = render_report(cfg.date, cfg.data_dir, cfg.t0, cfg.t1, res, timings, note, th)
    (cfg.out_dir / "quality.md").write_text(md)
    (cfg.out_dir / "quality.json").write_text(json.dumps([c.__dict__ for c in res.checks], indent=1, default=str))
    con.close()
    shutil.rmtree(cfg.out_dir / "tmp", ignore_errors=True)  # DuckDB spill files; nothing to keep
    return res


def summary_line(res: Result) -> str:
    counts = {s: sum(1 for c in res.checks if c.status == s) for s in (FAIL, WARN, PASS, SKIP)}
    return " · ".join(f"{v} {k}" for k, v in counts.items())
