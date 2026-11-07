# Replay tool: order book reconstruction and validation

Date: 2026-11-07, revised the same day for D-020/D-021 (fill matching, window ends). Code: `src/polymm/replay.py`, CLI `polymm replay`, tests `tests/test_replay.py`.

## 1. What was built

`polymm.replay` rebuilds the L2 order book of any recorded token at any past
instant from the stored `book_events` (snapshots + deltas), by feeding them in
`ts_recv_ns` order through the same `BookState` the live recorder uses. On top
of that it writes five derived Parquet tables and validates the reconstruction
against the independently recorded REST snapshots.

### API

| Call | Purpose |
|---|---|
| `iter_books(data_dir, asset_id, t0, t1)` | yields `BookAt(ts_recv_ns, book, valid, event_type)` after every event of the asset in `[t0, t1)`; replay starts at the last snapshot before `t0` (looked up to 6 h back), so the first yielded book is complete |
| `book_at(data_dir, asset_id, t)` | the book as of `t` (events with `ts_recv_ns <= t` applied), or `None` if no snapshot precedes `t` |
| `Replayer(ReplayConfig(...)).run()` | batch driver: replays every token in a window, writes `<out>/derived/*`, returns stats; `rest_checks_all` / `ws_mismatches` feed `validation_report()` |
| `polymm replay --date D --out DIR [--asset A] [--t0 --t1] [--chunk-s] [--validate-only] [--data ROOT]` | CLI; prints the validation report at the end |

### Derived tables (`<out>/derived/<table>/dt=YYYY-MM-DD/hour=HH/part-*.parquet`, Hive layout like the input)

| Table | Row | Columns |
|---|---|---|
| `tob` | after every event per asset | `ts_recv_ns, asset_id, event_type, best_bid, bid_size, best_ask, ask_size, mid, microprice, spread, valid`. `valid` is false before the first snapshot, while stale after a reconnect, and when a side is empty or the book is crossed. `event_type` also takes `reconnect` (the invalidating row) and `resync` (book replaced after a confirmed gap). |
| `depth_1s` | one per (whole second, asset), as-of semantics: events with `ts_recv_ns <= second` included | wide: `bid_px_1..5`, `bid_cum_1..5`, `ask_px_1..5`, `ask_cum_1..5` (price of the k-th best level, CUMULATIVE size through level k; level size = `cum_k - cum_{k-1}`), `n_bid_levels`, `n_ask_levels`, `valid`. Rows stop 15 min after an asset's last event (unsubscribed BTC 15-minute windows would otherwise produce a row per second until midnight). |
| `events_labeled` | every delta (incl. mirrored ones, see §4) | `side, price, size_before, size_after, size_change, label ∈ {add, fill, cancel, unchanged}, trade_ts_recv_ns, trade_size, trade_lag_ns, mirrored, conn_id, n_trades, trade_size_sum`. Matching is trade → delta (D-020): each trade picks at most one size decrease per token, the nearest within ±1 s at its price on the side it consumed (taker BUY consumes the ask side) whose decrease is at least the trade size, else the nearest; several trades may pick one delta (`n_trades`, `trade_size_sum`; `trade_ts_recv_ns`/`trade_size`/`trade_lag_ns` describe the nearest one). `label = fill` only on picked deltas; `size_change + trade_size_sum` is what was cancelled on top. Trades are stored once per fill on the taker's token, so the sibling's deltas are matched against the mirrored trade (YES BUY at p ≡ NO SELL at 1−p). |
| `clean_windows` | per asset, `[t_start, t_end)` where the reconstruction is trusted | `start_kind ∈ {snapshot_ws, snapshot_rest, rest_resync}`, `end_reason ∈ {reconnect, idle, rest_gap, ws_gap, end}`, `last_verified` (ts of the last REST or WS snapshot that matched inside the window), `n_events`. A token's last window ends at its last event (D-021); `idle` = the token had no event for 15 min before a reconnect, i.e. was unsubscribed. |
| `rest_checks` | one per REST snapshot | `in_window, matched, kind ∈ {exact, before, after, init, gap}, lag_ns, n_rest_levels, n_diff, reason` |

Read with DuckDB `read_parquet('<out>/derived/tob/**/*.parquet', hive_partitioning=1)` or polars `scan_parquet`.

### Key design decisions

- **Files are selected by Parquet statistics, not by partition.** The recorder's `hour=` is the hour of a flush's *first* row; after a laptop sleep, a file in `hour=00` held rows from 01:29. `scan_files` reads `ts_recv_ns` min/max from every footer (0.4 ms per file) and the driver works in time chunks (default 10 min), so ordering is exact across files and memory stays at ~1.5 GB peak.
- **Stable sort on `ts_recv_ns` only.** Rows of one frame share `ts_recv_ns`; their file order is the message order (bids before asks in a snapshot, `price_changes` order in a delta) and must survive.
- **Markets are replayed as pairs** (both tokens together), which is what the A1 mirror rule needs (§4) and is a no-op on pre-A1 data.
- **Tolerant REST matching.** A REST read can be ~100 ms behind or ahead of the stream. Each book keeps an O(1)-updatable fingerprint of its level set (XOR of per-level hashes) plus the last second of `(ts, server hash, fingerprint)`. A REST snapshot matches if it equals the current book (`exact`), any state in the previous second (`before`), or any state in the following second (`after`, decided when the stream passes the deadline). Only then is it a `gap`.
- **Gap handling re-applies deltas.** On a confirmed gap the book is replaced by the REST levels and the deltas received since 1 s before the REST read are re-applied. Deltas are absolute level sizes, so re-applying one the REST book already reflects is harmless, and the ones it does not reflect bring the book to the present. The old window ends at the REST timestamp; the new one starts when the gap was confirmed (the second in between is untrusted).
- **WS snapshots are witnesses too.** A delta dropped between two REST polls can be healed by a spontaneous WS `book` snapshot before the poller sees it; the REST rate would then overstate cleanliness. A WS snapshot that matches neither the current book nor any state in the previous second ends the window (`ws_gap`) and starts a new one.
- **`BookState` was not modified.** Replay needs a cached top of book (recomputing `max(bids)` per event is O(levels)) and the fingerprint; both live in `replay._Asset` next to the `BookState`, which stays the single owner of the apply semantics.

## 2. Validation

Window: the clean block **2026-11-06 13:10 → 2026-11-07 00:25 UTC** (laptop copy of the
dataset), all 166 tokens that had events, 53.9 M `book_events` rows. Every `snapshot_rest`
row group in the window was compared with the reconstructed book; a match is a REST book
equal to the stream's book at some instant within ±1 s of the REST receive time.

```
REST snapshot checks: 4947 total, 0 used as anchors (book not initialised / stale)
in clean windows: 4947 checks, 4947 matched = 100.000 %
  by kind: exact=3946, before=499, after=502
  timing races: lag p5=-368 ms p50=0 ms p95=0 ms (negative = REST behind the stream)

per family:                       per hour (UTC):
  btc_15m            509/509        13:00  446/446    19:00  380/380
  btc_daily_ladder  3742/3742       14:00  604/604    20:00  386/386
  fed_dec_2026       696/696        15:00  504/504    21:00  378/378
                                    16:00  527/527    22:00  364/364
                                    17:00  400/400    23:00  420/420
                                    18:00  398/398    00:00  140/140
mismatches: 0
```

- **100.0 % (4,947 / 4,947), target ≥ 99.5 %.** 79.8 % of REST books equal the replayed
  book at the instant they arrive; 10.1 % equal a state from the previous second (REST
  read older than deltas already applied, typically 30–370 ms); 10.1 % equal a state
  reached within the following second (REST ahead of the stream). No REST book failed to
  appear in the stream, so no REST-confirmed gap and no resync happened in the block.
- The earlier quick check quoted in the task ("1,156 exact + 656 timing races out of
  1,411") was the live poller's counters, which count every poll, not only emitted
  snapshots; the numbers here are over the stored `snapshot_rest` rows.
- **Stream self-check.** Every WS `book` snapshot was also compared with the book before
  it was applied: 121,767 exact, 372 equal to a state in the previous second, **81
  mismatches**. The mismatches are not scattered: they sit in a handful of instants
  (14:25:03, 14:31:51, 14:40:06, …) where the server sent a `book` snapshot for all 82
  subscribed tokens at once after ~9 s with no events on the whole stream. Diagnosis on
  14:40:06.514: the asset's last event was 8,987 ms earlier, the burst snapshots carry
  `ts_exch_ms` spread over the preceding 37 s, the pre-burst book differs from the snapshot
  in 35 levels, and applying the later deltas without the snapshot never converges (the
  differing levels are mostly never touched again). So these are **real stalls whose deltas
  were lost**, healed server-side by the mass snapshot before the next REST poll, which is
  why the REST rate alone would overstate cleanliness. Replay therefore ends the clean
  window at the asset's last applied event (`end_reason = ws_gap`) and starts a new one at
  the snapshot.
- **Clean windows in the block** (after D-021): 845 windows; 606 end at one of the 8
  reconnects, 88 end `idle` at the token's last event before a reconnect (unsubscribed BTC
  15-minute tokens), 81 at a `ws_gap`, 70 at the token's last event before the end of the
  block. Total window time 766 h, down from 874 h when unsubscribed tokens kept a window
  until the end of the range. Server-reported `best_bid/best_ask` on deltas agree
  with the replayed top of book on 99.84 % of deltas in hour 15 (DATA_PLAN quoted ~99.6 %);
  the replayed book is the one that matches the snapshots, so it is the reference.
- **A1-era data (mirror rule on real data):** 2026-11-07 09:00 → 11:28, where outcome-1
  tokens are stored only where they differ from mirror(outcome 0): 938 / 938 REST checks
  matched (btc_15m 89, btc_daily_ladder 745, fed_dec_2026 104), 74 REST snapshots used as
  anchors after reconnects. Outcome-1 books rebuilt by mirroring pass the same test as
  outcome-0 books, so the rebuild rule is confirmed end to end, not only on raw frames.

## 3. Performance

Measured on the laptop (8 GB RAM, 8 cores, one core used), standalone run through the CLI:

| | |
|---|---|
| rows replayed (block, 11 h 15 min) | 53.9 M (42.2 M deltas, 123 k WS snapshots, 4,947 REST snapshots, 694 reconnect-marked books) |
| wall time | 751 s (12.5 min) with the laptop swapping heavily (5.8 of 7 GB swap in use by other apps, replay got ~16 % of a core); 477 s of CPU time (user+sys). The same code on an unloaded machine replayed one hour (7.9 M rows) in 43 s. |
| throughput | 72 k rows/s loaded; 185 k rows/s unloaded |
| peak RSS | 1.64 GB (10-minute chunks) |
| time split (loaded run) | read 35 s, Python event loop 398 s, labelling + Parquet writing 318 s |
| output size (block) | tob 302 MB (42.3 M rows), events_labeled 340 MB (42.2 M), depth_1s 38 MB (2.85 M), clean_windows 16 KB (845 windows), rest_checks 540 KB |

A full day of ~54 M rows therefore takes about 5 minutes unloaded and stayed under the
15-minute budget even on the swapping machine. The Python loop costs ~3 µs per event (BookState.apply_delta, fingerprint update,
cached top-of-book, history deque, one tuple per output row). Reading is polars
(`read_parquet` per file + stable sort), chunked by time (10 min ≈ 1.4 M rows) so memory
stays flat. Writing is ~40 % of the time (Python tuples → Arrow); the next win, if needed,
is building the `tob` columns in Arrow directly or dropping rows whose top of book did not
change (`tob` has one row per event by design, 42 M rows for the block).

## 4. Mirror rule (A1) and the two data eras

From 2026-11-07 ~11:00 UTC the recorder stores outcome 1 of a market only where it
is not the exact mirror of outcome 0 (DATA_PLAN §5 A1). Replay applies the rebuild
rule per delta frame: if the frame (same `ts_recv_ns`) has rows for the sibling,
they are used; otherwise the outcome-0 rows are mirrored (side flipped, price
`1 − p` via `book.flip_price`). Outcome-1 snapshots are only stored when they change
something, so nothing needs mirroring there. On 2026-11-06 data both tokens are
always present, so `mirrored_deltas` is 0 and each token is effectively replayed
from its own rows; the validation below therefore tests the engine, not the mirror
rule. The mirror rule is covered by the synthetic tests; validating it on real A1-era
data (REST checks of outcome-1 tokens from 2026-11-07 on) is a follow-up.

## 5. Known limitations and follow-ups

- **Frames with several `price_change` messages.** The mirror decision is per
  `ts_recv_ns`. If one WebSocket frame carried two `price_change` messages for the same
  market and the recorder dropped outcome-1 rows for only one of them, replay would skip
  mirroring the other. Not observed; would show up as a REST mismatch on the outcome-1
  token in A1-era data (0 / 938 so far).
- **Where a gap started is unknown.** A `rest_gap` ends the window at the REST timestamp
  and a `ws_gap` at the last applied event; the dropped delta may lie anywhere after
  `last_verified`. Strict users should trim windows to `[t_start, last_verified]`.
- **15 min of silence means unsubscribed** (`grid_max_idle_s`, D-021): the depth grid stops,
  a later reconnect writes no `reconnect` row and closes the window at the last event
  (`idle`). A subscribed token silent for longer than that (REST polls failing for 15 min)
  would be treated the same way; its next snapshot simply opens a new window.
- **Fill labels are a heuristic** (D-020, revised after `polymm quality` found 11.4 fill
  rows per trade with the first delta → trade rule): trade → delta, one delta per trade and
  token, nearest in time with a size filter. A delta can still be mislabelled when a cancel
  at the same price lands closer to the trade message than the real fill, or when a level
  is hit and re-quoted within one delta. Measured with
  `polymm quality` on the 2026-11-06 block, before → after the revision: fill rows per
  labelled trade 10.88 → 1.001 (btc_15m 11.44 → 1.001, ladder 1.59 → 1.00, Fed 1.45 → 1.00);
  fill rows that shrink by exactly the trade size 7.9 % → 44.0 %; "trade has a size
  decrease at its price within 1 s" unchanged at 99.77 % (it is computed from the raw
  decreases, not from the labels). 103,330 fill deltas absorb 121,970 trades: 87 % carry one
  trade, 10 % two, 3 % three or more (one taker order hitting several makers). For deltas
  with several trades the decrease is on average smaller than `trade_size_sum` (the size
  filter is applied per trade, not per group, and a level can be hit and partly re-quoted
  inside one delta), so `size_change + trade_size_sum` is only an estimate of the cancelled
  part. Quality after the revision: 0 FAIL, 1 WARN (ladder taker-side consistency 95.1 %,
  unchanged from before), 67 PASS.
- **`book_at` / `iter_books` re-scan Parquet footers per call** (~0.5 s for a day of files);
  fine for ad-hoc use, use `Replayer` for anything that loops over many instants.
- **Derived output location.** The block's tables were written to
  `reports/replay-block/derived/` in the replay worktree (gitignored); rerun
  `polymm replay --date 2026-11-06 --data <root>/data --t0 2026-11-06T13:10 --t1 2026-11-07T00:25 --out <dir>`
  (≈ 5–12 min) to regenerate elsewhere.
- Not changed: `book.py`, the recorder modules, `records.py`, `config.yaml`, `ops/`.
