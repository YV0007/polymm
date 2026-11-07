"""`polymm audit --date YYYY-MM-DD`: is the data for that day trustworthy?

Reads the Parquet tree with DuckDB and prints, per table and per asset,
what the recorder captured and where the stream broke. This is the first
thing to run every morning and the first thing to consult before any analysis.
"""
from __future__ import annotations

from datetime import datetime, timezone

import duckdb

from .config import Config


def _q(con, sql: str):
    try:
        return con.sql(sql)
    except duckdb.IOException:
        return None
    except duckdb.CatalogException:
        return None
    except duckdb.BinderException:  # column absent on days before it was added
        return None


def run_audit(cfg: Config, date: str | None) -> int:
    date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    d = cfg.data_dir
    con = duckdb.connect()
    print(f"== audit {date}  ({d})\n")

    def glob(table: str, layer: str = "events") -> str:
        return f"'{d}/{layer}/{table}/dt={date}/**/*.parquet'"

    # ---- health: uptime, reconnects, gaps ---------------------------------------
    h = _q(con, f"""
        select count(*) as samples,
               to_timestamp(min(ts_ns)/1e9) as first, to_timestamp(max(ts_ns)/1e9) as last,
               max(reconnects) as reconnects, max(gaps) as gaps, max(tob_mismatches) as tob_mismatches,
               max(parse_errors) as parse_errors, max(msgs_total) as msgs_total, max(rows_delta) as rows_delta,
               max(msgs_trade) as trades, max(queue_max) as queue_max,
               round(max(write_latency_ms)) as max_write_ms,
               round(max(bytes_raw)/1e6,1) as raw_mb, round(max(bytes_parquet)/1e6,1) as parquet_mb
        from read_parquet({glob('health')}, hive_partitioning=1, union_by_name=1)
    """)
    print("-- health (max of cumulative counters over the day's runs)")
    print(h if h is not None else "  no health files\n")

    # Why per-run: counters reset when the process restarts; a drop in msgs_total
    # between consecutive samples marks a restart, which is itself a gap to know about.
    r = _q(con, f"""
        with s as (
          select ts_ns, msgs_total, lag(msgs_total) over (order by ts_ns) as prev
          from read_parquet({glob('health')}, hive_partitioning=1, union_by_name=1))
        select count(*) filter (where prev is not null and msgs_total < prev) as process_restarts from s
    """)
    if r is not None:
        print(r)

    # Decision A1: outcome-1 rows are stored only where they are NOT the exact
    # mirror of outcome 0. `kept` should stay a tiny fraction of `dropped`;
    # a jump means Polymarket's mirroring changed and needs a look.
    mr = _q(con, f"""
        select max(mirror_dropped) as mirror_dropped, max(mirror_kept) as mirror_kept,
               round(100.0 * max(mirror_kept) / nullif(max(mirror_dropped) + max(mirror_kept), 0), 3) as kept_pct
        from read_parquet({glob('health')}, hive_partitioning=1, union_by_name=1)
    """)
    if mr is not None:
        print("-- mirror (A1): outcome-1 rows dropped vs kept (max over the day's runs)")
        print(mr)

    # ---- coverage per hour --------------------------------------------------------
    cov = _q(con, f"""
        select hour, count(*) as rows, count(distinct asset_id) as assets,
               count(*) filter (where event_type='delta') as deltas,
               count(*) filter (where event_type='snapshot_ws') as snap_ws,
               count(*) filter (where event_type='snapshot_rest') as snap_rest,
               count(*) filter (where event_type='reconnect') as reconnects
        from read_parquet({glob('book_events')}, hive_partitioning=1, union_by_name=1)
        group by hour order by hour
    """)
    print("-- book_events per hour (UTC)")
    print(cov if cov is not None else "  no book_events files\n")

    # ---- per asset ----------------------------------------------------------------
    pa_ = _q(con, f"""
        with be as (select * from read_parquet({glob('book_events')}, hive_partitioning=1, union_by_name=1)),
             tr as (select asset_id, count(*) as trades, sum(size) as traded_size
                    from read_parquet({glob('trades')}, hive_partitioning=1, union_by_name=1) group by 1),
             mk as (select asset_id, any_value(family) as family, any_value(outcome) as outcome,
                           any_value(question) as question
                    from read_parquet('{d}/reference/markets/**/*.parquet', hive_partitioning=1, union_by_name=1) group by 1)
        select mk.family, mk.outcome, left(mk.question, 48) as question,
               count(*) filter (where be.event_type='delta') as deltas,
               count(*) filter (where be.event_type like 'snapshot%') as snap_rows,
               coalesce(tr.trades, 0) as trades, round(coalesce(tr.traded_size,0)) as traded_sz,
               strftime(to_timestamp(min(be.ts_recv_ns)/1e9), '%H:%M:%S') as first, strftime(to_timestamp(max(be.ts_recv_ns)/1e9), '%H:%M:%S') as last,
               round(avg(be.ts_recv_ns/1e6 - be.ts_exch_ms)) as lat_ms
        from be left join tr using (asset_id) left join mk using (asset_id)
        where be.asset_id <> ''
        group by 1,2,3,tr.trades,tr.traded_size order by 1,3,2
    """)
    print("-- per asset")
    print(pa_ if pa_ is not None else "  (missing tables)\n")

    # ---- latency sanity -----------------------------------------------------------
    lat = _q(con, f"""
        select round(quantile_cont(ts_recv_ns/1e6 - ts_exch_ms, 0.5)) as p50_ms,
               round(quantile_cont(ts_recv_ns/1e6 - ts_exch_ms, 0.99)) as p99_ms,
               round(min(ts_recv_ns/1e6 - ts_exch_ms)) as min_ms
        from read_parquet({glob('book_events')}, hive_partitioning=1, union_by_name=1)
        where event_type='delta'
    """)
    print("-- recv - exchange latency on deltas (negative min = clock skew)")
    print(lat if lat is not None else "")
    return 0
