"""Table schemas: one place that defines what every Parquet table looks like.

Why explicit pyarrow schemas (not inferred): inference from a batch with all-null
`best_bid` would produce a `null` typed column and later batches would not
append to the same dataset cleanly. Fixed schemas make every file identical.

Rows are plain tuples in schema column order (fast to append at ~1k rows/s);
the schema is the single source of truth for column names and types.

Schema evolution rule: only ever ADD columns, at the END. Readers must pass
`union_by_name=true` (DuckDB) / `missing_columns='insert'` (polars); the default
readers fail with "schema mismatch" as soon as old and new files are mixed.
"""
from __future__ import annotations

import pyarrow as pa

# ---- events layer -----------------------------------------------------------

BOOK_EVENTS = pa.schema(
    [
        ("ts_recv_ns", pa.int64()),  # local clock, taken before parsing
        ("ts_exch_ms", pa.int64()),  # exchange timestamp from the message
        ("asset_id", pa.string()),
        ("condition_id", pa.string()),
        # delta | snapshot_ws | snapshot_rest | reconnect
        ("event_type", pa.string()),
        ("snapshot_id", pa.int64()),  # groups the levels of one snapshot; null for deltas
        ("side", pa.string()),  # BUY = bid side, SELL = ask side
        ("price", pa.float64()),
        ("size", pa.float64()),  # new TOTAL size resting at this level (0 = level removed)
        ("hash", pa.string()),
        ("best_bid", pa.float64()),  # only on deltas (server-side top of book)
        ("best_ask", pa.float64()),
        ("tick_size", pa.float64()),  # only on snapshots
        ("conn_id", pa.int32()),
    ]
)

TRADES = pa.schema(
    [
        ("ts_recv_ns", pa.int64()),
        ("ts_exch_ms", pa.int64()),
        ("asset_id", pa.string()),
        ("condition_id", pa.string()),
        ("price", pa.float64()),
        ("size", pa.float64()),
        ("side", pa.string()),  # VERIFY: taker side (BUY = taker bought this asset)
        ("fee_rate_bps", pa.int32()),
        ("tx_hash", pa.string()),
        ("conn_id", pa.int32()),
    ]
)

TOB = pa.schema(  # best_bid_ask custom event: server-side top of book
    [
        ("ts_recv_ns", pa.int64()),
        ("ts_exch_ms", pa.int64()),
        ("asset_id", pa.string()),
        ("condition_id", pa.string()),
        ("best_bid", pa.float64()),
        ("best_ask", pa.float64()),
        ("spread", pa.float64()),
        ("conn_id", pa.int32()),
    ]
)

TICK_CHANGES = pa.schema(
    [
        ("ts_recv_ns", pa.int64()),
        ("ts_exch_ms", pa.int64()),
        ("asset_id", pa.string()),
        ("condition_id", pa.string()),
        ("old_tick", pa.float64()),
        ("new_tick", pa.float64()),
        ("conn_id", pa.int32()),
    ]
)

RESOLUTIONS = pa.schema(  # market_resolved custom event
    [
        ("ts_recv_ns", pa.int64()),
        ("ts_exch_ms", pa.int64()),
        ("condition_id", pa.string()),
        ("winning_asset_id", pa.string()),
        ("winning_outcome", pa.string()),
        ("conn_id", pa.int32()),
    ]
)

HEALTH = pa.schema(
    [
        ("ts_ns", pa.int64()),
        ("conn_id", pa.int32()),
        ("uptime_s", pa.float64()),
        ("n_subscribed", pa.int32()),
        ("msgs_total", pa.int64()),
        ("msgs_book", pa.int64()),
        ("msgs_delta", pa.int64()),
        ("rows_delta", pa.int64()),
        ("msgs_trade", pa.int64()),
        ("msgs_tob", pa.int64()),
        ("msgs_other", pa.int64()),
        ("parse_errors", pa.int64()),
        ("reconnects", pa.int64()),
        ("gaps", pa.int64()),  # REST hash != local hash
        ("tob_mismatches", pa.int64()),  # delta best_bid/ask != local book
        ("rest_polls", pa.int64()),
        ("queue_depth", pa.int32()),
        ("queue_max", pa.int32()),
        ("write_latency_ms", pa.float64()),
        ("bytes_raw", pa.int64()),
        ("bytes_parquet", pa.int64()),
        # Added 2026-11-07 (A1). Older files lack these: read with union_by_name.
        ("mirror_dropped", pa.int64()),  # outcome-1 rows not stored: exact mirror of outcome 0
        ("mirror_kept", pa.int64()),  # outcome-1 rows stored because no mirror matched
    ]
)

# ---- reference layer --------------------------------------------------------

MARKETS = pa.schema(  # written by UniverseWatcher on every subscribe/unsubscribe/refresh
    [
        ("fetched_at_ns", pa.int64()),
        ("action", pa.string()),  # subscribe | unsubscribe | refresh
        ("family", pa.string()),
        ("event_slug", pa.string()),
        ("market_slug", pa.string()),
        ("question", pa.string()),
        ("condition_id", pa.string()),
        ("asset_id", pa.string()),
        ("outcome", pa.string()),
        ("start_date", pa.string()),
        ("end_date", pa.string()),
        ("tick_size", pa.float64()),
        ("min_order_size", pa.float64()),
        ("neg_risk", pa.bool_()),
        ("fees_enabled", pa.bool_()),
        ("fee_rate", pa.float64()),
        ("fee_rebate_rate", pa.float64()),
        ("rewards_min_size", pa.float64()),
        ("rewards_max_spread", pa.float64()),
        ("rate_per_day", pa.float64()),
        ("accepting_orders", pa.bool_()),
        # Added 2026-11-07 (A1): position in clobTokenIds. book_events stores
        # outcome 0 in full; outcome 1 = mirror(outcome 0) except where it has rows.
        ("outcome_index", pa.int32()),
    ]
)

TABLES: dict[str, pa.Schema] = {
    "book_events": BOOK_EVENTS,
    "trades": TRADES,
    "tob": TOB,
    "tick_changes": TICK_CHANGES,
    "resolutions": RESOLUTIONS,
    "health": HEALTH,
    "markets": MARKETS,
}

# Which layer directory each table lives under.
LAYER: dict[str, str] = {
    "book_events": "events",
    "trades": "events",
    "tob": "events",
    "tick_changes": "events",
    "resolutions": "events",
    "health": "events",
    "markets": "reference",
}
