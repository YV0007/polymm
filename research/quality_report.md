# Data-quality checks: what the recorded days are good for

Date: 2026-11-07. Code: `src/polymm/quality.py`, CLI `polymm quality`, tests `tests/test_quality.py`.
Reports: `reports/2026-11-06/quality.md` (laptop block) and `reports/2026-11-07/quality.md` (VPS, incl. Binance).
Every later analysis starts from `reports/<date>/clean_windows_strict.parquet`, not from `clean_windows`.

## 1. What was built

`polymm quality --date D [--data ROOT] [--replay-out DIR] [--out reports/D] [--t0 --t1]` replays the day
(or reuses `<replay-out>/derived/` when it exists), opens the replay's derived tables and the raw tables as
DuckDB views, runs seven groups of checks and writes one page of PASS / WARN / FAIL rows plus an "excluded"
list. Thresholds live in `THRESHOLDS` at the top of the module and are printed at the end of every report.

| group | what is measured | where it reads |
|---|---|---|
| coverage | share of subscribed time inside clean windows, loose `[t_start, t_end)` and strict `[t_start, last_verified]`, per family; windows by end reason; rows / tokens / snapshots per hour; thin hours | `tob`, `clean_windows`, `book_events` |
| book | crossed top of book (rows and time-weighted seconds), one side empty, spread > 0.20, all inside loose windows; the empty-side check is graded only on live BTC 15-minute windows (first 12 min) | `tob` |
| mirror | pre-A1: share of 1-s grid instants where YES bid = 1 − NO ask and YES ask = 1 − NO bid, with sizes; A1 era: kept outcome-1 delta rows classified against the outcome-0 rows of the same frame | `depth_1s`, `book_events`, `health` |
| trades | pre-trade book from the nearest size decrease at the trade's price (own or sibling token, ±1 s): inside/at the spread, taker side consistent, wrong side, trade has a size decrease, and the replay labeller's fill rows per trade | `trades`, `events_labeled`, `tob` |
| latency | recv − exchange ts on deltas: p50/p95/p99 per family and per hour, negatives, drift (slope of per-minute p5), trade latency | `book_events`, `trades` |
| spot | Binance `update_id` monotonic, `trade_id` holes, recv − event_ms, reconnect markers, quote gaps > 5 s, direction sanity vs the live Up token | `spot_quotes`, `spot_trades`, `depth_1s` |
| xfeed | cross-correlation of 1-s returns, Binance log-mid vs live Up-token mid, lags −30..+30 s; lag of max correlation (positive = Binance leads) | `spot_quotes`, `depth_1s` |

Design choices worth knowing (all explained in `# Why:` comments):

- **DuckDB views, SQL checks.** `tob` has 42 M rows for the block; the checks aggregate in one pass per
  group and never hold the table in Python. Quantiles over 40 M latencies come from a 0.1 ms histogram
  (the first version used `quantile_cont` and ran out of memory at 3 GB).
- **Time weights.** `tob` has a row per event, so every "share" is also reported in seconds using the time
  until the asset's next row; busy minutes do not dominate.
- **Windows are clipped to the token's event span**, and **outcome-1 windows inherit the sibling's
  verification** (see §4 for why both are needed).
- **Tests register tiny polars frames under the table names**, so the SQL under test is the production SQL.

## 2. Results

### 2026-11-06, laptop, 13:10 → 00:25 UTC (54 M rows, 8 reconnects): 2 FAIL · 2 WARN · 63 PASS · 2 SKIP

| check | all | btc_15m | btc_daily_ladder | fed_dec_2026 |
|---|---|---|---|---|
| clean share loose / strict | 99.9 % / 92.8 % | 99.9 % / 92.4 % | 99.9 % / 92.4 % | 100 % / 95.5 % |
| crossed book (time) | 1.7 s of 764 h | 1.7 s | 0.1 s | 0 s |
| live 15 m window one side empty (first 12 min) | – | 0.59 % | – | – |
| YES/NO mirrored on the 1 s grid (prices; prices+sizes) | 100 % / 100 % (1.16 M instants) | 100 % | 100 % | 100 % |
| trades inside/at spread; side consistent; wrong side | 96.1 % / 99.2 % / 0.06 % | 96.2 / 99.4 / 0.05 | 91.1 / **95.1 WARN** / 0.40 | 99.1 / 99.6 / 0 |
| trade has a size decrease at its price within 1 s | 99.8 % | 99.9 % | 95.6 % | 99.2 % |
| fill rows per labelled trade (replay) | **10.9 FAIL** | **11.4 FAIL** | 1.59 WARN | 1.45 |
| delta latency p50 / p95 / p99 | 16.6 / 155 / 554 ms | 17.4 / 158 / 551 | 15.1 / 148 / 567 | 15.2 / 134 / 418 |
| negative latency; clock drift | 0; +0.15 ms/h | | | |
| spot, xfeed | SKIP (no spot tables on the laptop) | | | |

Windows: 845 (694 end at a reconnect, 81 at a WS stall, 70 at the end of the block); 84 reconnect-ended
windows were never verified (too short for a REST anchor). Excluded: those 84 windows (strict length 0) and
4,094 of 61,053 trades whose reference instant is outside a clean window or whose book is not valid.

### 2026-11-07, VPS, 09:35 → 11:55 UTC (5.6 M rows, 1 reconnect; Binance from 11:39): 2 FAIL · 4 WARN · 71 PASS · 1 SKIP

| check | all | btc_15m | btc_daily_ladder | fed_dec_2026 |
|---|---|---|---|---|
| clean share loose / strict | 96.1 % / 84.0 % | 99.8 % / 54.2 %* | 96.4 % / 88.9 % | 90.3 % / **82.3 % WARN** |
| crossed book (time) | 0.1 s of 200 h | 0.1 s | 0.0 s | 0 s |
| live 15 m window one side empty (first 12 min) | – | 0.0 % (10 windows) | – | – |
| YES/NO mirrored, pre-A1 part (until 10:59:08) | 100 % (148 k instants) | 100 % | 100 % | 100 % |
| A1: kept outcome-1 deltas / outcome-0 deltas (from 10:59:08) | 0.00 % of 796 k | 0 | 0 | 0 |
| trades inside/at spread; side consistent; wrong side | 94.8 / 99.3 / 0.07 % | 94.9 / 99.3 / 0.07 | **89.4 WARN** / **97.7 WARN** / 0 | 100 / 100 / 0 |
| trade has a size decrease at its price within 1 s | 99.9 % | 99.9 % | 97.6 % | 98.4 % |
| fill rows per labelled trade (replay) | **7.7 FAIL** | **7.9 FAIL** | 1.54 WARN | 1.18 |
| delta latency p50 / p95 / p99 | 10.8 / 25 / 109 ms | 11.0 / 27 / 107 | 10.5 / 20 / 113 | 10.7 / 18 / 120 |
| negative latency; clock drift | 0; +0.81 ms/h (r² 0.54, 141 minutes only) | | | |
| Binance quotes: update_id violations / reconnects / gaps > 5 s | 0 / 0 / 0 (max gap 1.1 s, 20 k quotes) | | | |
| Binance trades: id holes / recv − event p50 / p99 | 0 / 128 ms / 149 ms (4,782 trades) | | | |
| Binance vs Polymarket lead | max corr 0.35 at **+1 s** (corr 0.18 at 0 s), 847 s, 11 windows: PASS | | | |

\* btc_15m strict share is 54 % with the family threshold at 60 % only because the day ends at 11:55 while
five 15-minute tokens are mid-life (their last REST anchor is up to 10 min back); the sibling-inherited
numbers per token average 83 %. The same family reaches 92 % on the full laptop block.

## 3. What is excluded, and why

- **Everything after a window's last verified instant.** Replay only learns that a delta was lost when the
  next snapshot disagrees; the loss can sit anywhere after the last agreeing snapshot. With REST anchors
  every 10 min this costs ~7 % of the time on a healthy day (loose 99.9 % → strict 92.8 %).
- **Reconnect-ended windows with no snapshot in them** (84 on 11-06, 1 on 11-07): reconnect storms produce
  windows of a few minutes that never see an anchor. Strict length 0.
- **Trades whose reference instant is outside a clean window or whose book is not valid** (6.7 % on 11-06,
  4.9 % on 11-07): trades around reconnects and stalls, and trades on tokens whose book has one side empty
  (a 15-minute market at 0.99 in its last minutes).
- **One-sided books outside live 15-minute windows** are reported but not graded: far ladder strikes have no
  bid for hours and unstarted 15-minute windows have no quotes; that is the market, not the recorder.
- **The direction sanity check** (Binance above its open ⇒ Up token above 0.5) had only one window with a
  Binance open on 11-07 and is SKIP; it needs a full day of spot data.

## 4. Things that look like recorder or replay issues (described, not fixed)

1. **Replay `clean_windows` ends a token's last window at `t1`, not at its last event.** A BTC 15-minute token
   unsubscribed at 10:00 gets a window to midnight (`end_reason = end`), and every initialised book also gets a
   `reconnect` row in `tob` at each later reconnect. Time-weighted numbers computed naively from those tables
   counted 14 h of a book nobody recorded (the first run reported 30 % "empty side" time on the VPS day).
   `quality` clips windows to the span of the token's non-reconnect events. Suggest: replay closes the window at
   the last event (or the recorder writes an explicit unsubscribe marker).
2. **Replay `events_labeled` over-labels fills.** The labeller gives every size decrease at the trade's price
   within ±1 s the trade's label: 11.4 fill rows per trade on btc_15m (1.16 M "fills" for 107 k trades), and
   only 8 % of fill rows shrink by exactly the trade size. The first delta carrying a trade's label is often a
   cancel 300 ms earlier (seen in the data: a quote-pull across all levels tagged as a fill of a 24-share trade
   that actually consumed a level 0.8 ms before the message). Also the converse: one taker order that hits
   several makers at one level produces several `last_trade_price` messages and ONE delta, so with the
   labeller's one-trade-per-delta rule 12 % of trades looked unmatched although 99.9 % have a size decrease at
   their price within 1 s. Anything that counts fills (fill rate, queue model, markouts per fill) must not use
   `label = 'fill'` as is. Suggest: one-to-one matching trade → nearest size decrease, size-aware, and a
   `n_trades` column per delta.
3. **A1-era outcome-1 tokens are never verified on their own.** The recorder drops outcome-1 REST snapshots that
   are redundant (mirror of outcome 0), so after 2026-11-07 ~11:00 UTC the NO/Down windows have
   `last_verified = null` in `clean_windows`. A strict reader of the raw table would discard every outcome-1
   token. `quality` lets a window inherit the sibling's verification (the mirror rebuild was validated 938/938
   in `replay_report.md`); `clean_windows_strict.parquet` carries `verified_own` so this is visible.
4. **Laptop recorder stalls of 5–60 s.** On 11-06 the delta latency p99 is 0.5 s overall but 4.9 s in hour 18
   and 26 s in hour 23: in minute 23:47 the *median* delta arrived 17.7 s after the exchange timestamp
   (max 59 s), in 18:51 5.2 s. A whole minute delayed is the consumer, not the network. The VPS day has
   p99 = 110 ms with no such minute. These stalls coincide with the `ws_gap` windows (81 on 11-06). The
   laptop data is still usable inside strict windows, but any latency- or queue-sensitive analysis should use
   VPS data only.
5. **WS stalls happen on the VPS too.** 84 WS snapshot mismatches on 11-07, all at 11:39:14 after ~9 s of
   silence, the same "mass `book` after silence" pattern as on the laptop. It is server-side (or path-side),
   so the recorder's health row should count it as a gap, as the replay hand-over already suggested.
6. **`transaction_hash` is unique per trade message** (59,978 of 59,978 on 11-06, 11,932 of 11,932 on 11-07),
   although 3.8 % of btc_15m trades print beyond the touch, i.e. multi-level sweeps exist. DATA_PLAN §1.2 D5
   assumes a sweep shares one hash across its messages; it does not in this feed. Sweep detection must use time
   proximity (same token, same side, within a few ms).
7. **Taker side semantics confirmed** (`records.py` marks it VERIFY): BUY trades print at or above the best
   ask 99.4 % of the time on btc_15m, so `side = BUY` means the taker bought this token. The 0.05 % wrong-side
   trades inspected on the VPS day sat a few hundred ms after a book change (matching latency), not sign errors.
8. **Binance on a Saturday.** From 11:39 to 11:42 the BTC/USDT book sat at 76026.43 / 76026.44 with only the
   quantities moving; both the quotes and the trades say so, so the feed is fine, the market was pinned.
   Binance leads the Polymarket 15-minute mid by 1 s at the 1-s grid (corr 0.35 vs 0.18 at lag 0), which is
   the right sign for the clocks; 16 minutes of overlap is not enough to say more.
9. **Strict coverage is bounded by the REST anchor interval.** Each window end loses up to
   `rest_anchor_interval_s` (600 s). Halving it for short-lived families would raise strict coverage of
   btc_15m from ~92 % to ~96 % for a trivial amount of extra REST traffic; a config decision, not a code change.

## 5. Runtime

| day | rows | replay | checks | end to end | peak memory |
|---|---|---|---|---|---|
| 2026-11-06 laptop block (11 h 15 min) | 53.9 M | 7 min 33 s (4 min 45 s standalone on an idle machine) | 2 min 07 s (book 75 s, trades 44 s, latency 6 s); 4 min 03 s with another run competing | 9 min 45 s | DuckDB capped at 3 GB, spills to `reports/<date>/tmp` |
| 2026-11-07 VPS (2 h 20 min) | 5.6 M | 52 s | 11 s | 1 min 05 s | – |

A full VPS day (~55 M rows) extrapolates to ~5 min replay + ~5 min checks, within the 20-minute budget. The
book check dominates (one ASOF join plus a window function over 42 M rows, done twice: aggregates and spread
histogram); the next win would be writing `dur_ns` into `tob` at replay time.

## 6. How to use the output

```python
import polars as pl
sw = pl.read_parquet("reports/2026-11-06/clean_windows_strict.parquet")
# columns: asset_id, family, t_start, t_end (= last verified instant), start_kind, end_reason,
#          n_events, verified_own, t_end_loose, asset_strict_share
```

Join any per-event table on `asset_id` with `t_start <= ts_recv_ns <= t_end`. `asset_strict_share` is the
token's strict time over its subscribed time; tokens below ~0.5 are either very short-lived or sat in a
reconnect storm and are better left out of per-token statistics.
