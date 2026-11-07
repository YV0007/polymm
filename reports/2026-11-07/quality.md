# Data quality — 2026-11-07

Data root `/Users/macbook/Desktop/POLY MM/data_vps`; window 2026-11-07 00:00 → 2026-11-08 00:00 UTC. Replay ran in 52 s (5,623,473 rows, 1 reconnects, 84 WS mismatches, 0 REST resyncs). Checks took 11 s (coverage 0 s, book 5 s, mirror 1 s, trades 4 s, latency 1 s, spot 0 s, xfeed 0 s).

**2 FAIL · 4 WARN · 71 PASS · 1 SKIP**

| section | check | family | value | n | threshold | status | note |
|---|---|---|---|---|---|---|---|
| coverage | clean share (loose) | all | 95.88 % | 266 | info | **PASS** | 183.1 h of 190.9 h subscribed |
| coverage | clean share (strict) | all | 88.06 % | 266 | >= 85% / 70% | **PASS** | 168.1 h; 1 windows never verified; 0 tokens without a window |
| coverage | clean share (loose) | btc_15m | 99.68 % | 42 | info | **PASS** | 14.4 h of 14.5 h subscribed |
| coverage | clean share (strict) | btc_15m | 88.51 % | 42 | >= 60% / 40% | **PASS** | 12.8 h; 1 windows never verified; 0 tokens without a window |
| coverage | clean share (loose) | btc_daily_ladder | 96.36 % | 194 | info | **PASS** | 147.7 h of 153.3 h subscribed |
| coverage | clean share (strict) | btc_daily_ladder | 88.89 % | 194 | >= 85% / 70% | **PASS** | 136.2 h; 0 windows never verified; 0 tokens without a window |
| coverage | clean share (loose) | fed_dec_2026 | 90.33 % | 30 | info | **PASS** | 21.0 h of 23.2 h subscribed |
| coverage | clean share (strict) | fed_dec_2026 | 82.30 % | 30 | >= 85% / 70% | **WARN** | 19.1 h; 0 windows never verified; 0 tokens without a window |
| coverage | windows ending by end | all | 88 | 88 | info | **PASS** | 30.6 h loose, 24.2 h strict, 0 unverified |
| coverage | windows ending by reconnect | all | 94 | 94 | info | **PASS** | 88.7 h loose, 86.9 h strict, 0 unverified |
| coverage | windows ending by ws_gap | all | 84 | 84 | info | **PASS** | 63.8 h loose, 57.0 h strict, 1 unverified |
| coverage | thin hours | all | 0 | 3 | < 20% of median hour (1,224,337 rows) | **PASS** | none |
| book | crossed book (bid >= ask) | all | 0.1 s | 1,192 | <= 60 s / 600 s | **PASS** | 1,192 rows, 0.000 % of window time |
| book | one side empty (time share) | all | 27.40 % | 93,873 | info | **PASS** | 93,873 rows, 180565 s; structural for far strikes / unstarted windows |
| book | spread > 0.20 (time share) | all | 0.00 % | 860 | <= 1% | **PASS** | 860 rows, 1 s |
| book | crossed book (bid >= ask) | btc_15m | 0.1 s | 1,140 | <= 60 s / 600 s | **PASS** | 1,140 rows, 0.000 % of window time |
| book | one side empty (time share) | btc_15m | 7.15 % | 29,463 | info | **PASS** | 29,463 rows, 3717 s; structural for far strikes / unstarted windows |
| book | spread > 0.20 (time share) | btc_15m | 0.00 % | 860 | <= 1% | **PASS** | 860 rows, 1 s |
| book | crossed book (bid >= ask) | btc_daily_ladder | 0.0 s | 52 | <= 60 s / 600 s | **PASS** | 52 rows, 0.000 % of window time |
| book | one side empty (time share) | btc_daily_ladder | 33.26 % | 64,410 | info | **PASS** | 64,410 rows, 176848 s; structural for far strikes / unstarted windows |
| book | spread > 0.20 (time share) | btc_daily_ladder | 0.00 % | 0 | <= 1% | **PASS** | 0 rows, 0 s |
| book | crossed book (bid >= ask) | fed_dec_2026 | 0.0 s | 0 | <= 60 s / 600 s | **PASS** | 0 rows, 0.000 % of window time |
| book | one side empty (time share) | fed_dec_2026 | 0.00 % | 0 | info | **PASS** | 0 rows, 0 s; structural for far strikes / unstarted windows |
| book | spread > 0.20 (time share) | fed_dec_2026 | 0.00 % | 0 | <= 1% | **PASS** | 0 rows, 0 s |
| book | live 15 m window (first 720 s): one side empty (time share) | btc_15m | 0.01 % | 1,283,635 | <= 1% / 5% | **PASS** | 1 s of 6749 s in 10 live Up-token windows; crossed 0.0 s |
| mirror | pre-A1: YES/NO prices mirrored (1 s grid) | all | 100.00 % | 147,859 | >= 99.9% / 99% | **PASS** | until 10:59:08 UTC |
| mirror | pre-A1: prices and level-1 sizes mirrored | all | 100.00 % | 147,859 | info | **PASS** |  |
| mirror | pre-A1: YES/NO prices mirrored (1 s grid) | btc_15m | 100.00 % | 14,433 | >= 99.9% / 99% | **PASS** | until 10:59:08 UTC |
| mirror | pre-A1: prices and level-1 sizes mirrored | btc_15m | 100.00 % | 14,433 | info | **PASS** |  |
| mirror | pre-A1: YES/NO prices mirrored (1 s grid) | btc_daily_ladder | 100.00 % | 108,416 | >= 99.9% / 99% | **PASS** | until 10:59:08 UTC |
| mirror | pre-A1: prices and level-1 sizes mirrored | btc_daily_ladder | 100.00 % | 108,416 | info | **PASS** |  |
| mirror | pre-A1: YES/NO prices mirrored (1 s grid) | fed_dec_2026 | 100.00 % | 25,010 | >= 99.9% / 99% | **PASS** | until 10:59:08 UTC |
| mirror | pre-A1: prices and level-1 sizes mirrored | fed_dec_2026 | 100.00 % | 25,010 | info | **PASS** |  |
| mirror | A1: kept outcome-1 deltas / outcome-0 deltas | all | 0.00 % | 796,463 | <= 5% | **PASS** | from 10:59:08 UTC; kept 0 (none); 21 outcome-1 snapshots kept |
| mirror | A1: kept outcome-1 deltas / outcome-0 deltas | btc_15m | 0.00 % | 578,385 | <= 5% | **PASS** | from 10:59:08 UTC; kept 0 (none); 11 outcome-1 snapshots kept |
| mirror | A1: kept outcome-1 deltas / outcome-0 deltas | btc_daily_ladder | 0.00 % | 203,269 | <= 5% | **PASS** | from 10:59:08 UTC; kept 0 (none); 8 outcome-1 snapshots kept |
| mirror | A1: kept outcome-1 deltas / outcome-0 deltas | fed_dec_2026 | 0.00 % | 14,809 | <= 5% | **PASS** | from 10:59:08 UTC; kept 0 (none); 2 outcome-1 snapshots kept |
| trades | price inside/at the spread (pre-trade book) | all | 94.82 % | 11,352 | >= 90% / 80% | **PASS** | 11,932 trades, 580 not judged (outside windows / book not valid); 11,291 taker BUY; 5.1 % beyond the touch = multi-level sweeps |
| trades | side consistent (BUY >= ask, SELL <= bid) | all | 99.17 % | 11,352 | >= 99% / 95% | **PASS** |  |
| trades | wrong side (BUY < bid or SELL > ask) | all | 0.07 % | 11,352 | <= 0.5% / 2% | **PASS** | 8 trades |
| trades | trade has a size decrease at its price within 1 s | all | 99.86 % | 11,930 | >= 95% / 80% | **PASS** | 17 unmatched of 11,930 in windows |
| trades | price inside/at the spread (pre-trade book) | btc_15m | 94.86 % | 11,159 | >= 90% / 80% | **PASS** | 11,699 trades, 540 not judged (outside windows / book not valid); 11,121 taker BUY; 5.1 % beyond the touch = multi-level sweeps |
| trades | side consistent (BUY >= ask, SELL <= bid) | btc_15m | 99.18 % | 11,159 | >= 99% / 95% | **PASS** |  |
| trades | wrong side (BUY < bid or SELL > ask) | btc_15m | 0.07 % | 11,159 | <= 0.5% / 2% | **PASS** | 8 trades |
| trades | trade has a size decrease at its price within 1 s | btc_15m | 99.90 % | 11,699 | >= 95% / 80% | **PASS** | 12 unmatched of 11,699 in windows |
| trades | price inside/at the spread (pre-trade book) | btc_daily_ladder | 89.39 % | 132 | >= 90% / 80% | **WARN** | 172 trades, 40 not judged (outside windows / book not valid); 130 taker BUY; 10.6 % beyond the touch = multi-level sweeps |
| trades | side consistent (BUY >= ask, SELL <= bid) | btc_daily_ladder | 97.73 % | 132 | >= 99% / 95% | **WARN** |  |
| trades | wrong side (BUY < bid or SELL > ask) | btc_daily_ladder | 0.00 % | 132 | <= 0.5% / 2% | **PASS** | 0 trades |
| trades | trade has a size decrease at its price within 1 s | btc_daily_ladder | 97.65 % | 170 | >= 95% / 80% | **PASS** | 4 unmatched of 170 in windows |
| trades | price inside/at the spread (pre-trade book) | fed_dec_2026 | 100.00 % | 61 | >= 90% / 80% | **PASS** | 61 trades, 0 not judged (outside windows / book not valid); 40 taker BUY; 0.0 % beyond the touch = multi-level sweeps |
| trades | side consistent (BUY >= ask, SELL <= bid) | fed_dec_2026 | 100.00 % | 61 | >= 99% / 95% | **PASS** |  |
| trades | wrong side (BUY < bid or SELL > ask) | fed_dec_2026 | 0.00 % | 61 | <= 0.5% / 2% | **PASS** | 0 trades |
| trades | trade has a size decrease at its price within 1 s | fed_dec_2026 | 98.36 % | 61 | >= 95% / 80% | **PASS** | 1 unmatched of 61 in windows |
| trades | fill rows per labelled trade (replay over-labelling) | all | 7.741 | 162,632 | <= 1.5 / 3 | **FAIL** | 21,008 trades label 162,632 fill rows = 7.3 % of 2,219,683 size decreases; 8.1 % of fill rows shrink by exactly the trade size |
| trades | fill rows per labelled trade (replay over-labelling) | btc_15m | 7.876 | 162,002 | <= 1.5 / 3 | **FAIL** | 20,570 trades label 162,002 fill rows = 10.3 % of 1,574,296 size decreases; 7.9 % of fill rows shrink by exactly the trade size |
| trades | fill rows per labelled trade (replay over-labelling) | btc_daily_ladder | 1.535 | 488 | <= 1.5 / 3 | **WARN** | 318 trades label 488 fill rows = 0.1 % of 620,618 size decreases; 45.5 % of fill rows shrink by exactly the trade size |
| trades | fill rows per labelled trade (replay over-labelling) | fed_dec_2026 | 1.183 | 142 | <= 1.5 / 3 | **PASS** | 120 trades label 142 fill rows = 0.6 % of 24,769 size decreases; 38.0 % of fill rows shrink by exactly the trade size |
| latency | delta recv - exch p50 | all | 10.8 ms | 3,768,539 | <= 100 / 1000 ms | **PASS** | p95 25 ms, p99 109 ms |
| latency | delta recv - exch p99 | all | 109.2 ms | 3,768,539 | <= 2000 ms | **PASS** |  |
| latency | negative latency (recv before exch) | all | 0.00 % | 3,768,539 | <= 0% / 1% | **PASS** | 0 rows; min 6.9 ms |
| latency | delta recv - exch p50 | btc_15m | 11.0 ms | 2,599,986 | <= 100 / 1000 ms | **PASS** | p95 27 ms, p99 107 ms |
| latency | delta recv - exch p99 | btc_15m | 107.1 ms | 2,599,986 | <= 2000 ms | **PASS** |  |
| latency | negative latency (recv before exch) | btc_15m | 0.00 % | 2,599,986 | <= 0% / 1% | **PASS** | 0 rows; min 7.2 ms |
| latency | delta recv - exch p50 | btc_daily_ladder | 10.5 ms | 1,133,727 | <= 100 / 1000 ms | **PASS** | p95 20 ms, p99 113 ms |
| latency | delta recv - exch p99 | btc_daily_ladder | 113.4 ms | 1,133,727 | <= 2000 ms | **PASS** |  |
| latency | negative latency (recv before exch) | btc_daily_ladder | 0.00 % | 1,133,727 | <= 0% / 1% | **PASS** | 0 rows; min 6.9 ms |
| latency | delta recv - exch p50 | fed_dec_2026 | 10.7 ms | 34,826 | <= 100 / 1000 ms | **PASS** | p95 18 ms, p99 120 ms |
| latency | delta recv - exch p99 | fed_dec_2026 | 120.1 ms | 34,826 | <= 2000 ms | **PASS** |  |
| latency | negative latency (recv before exch) | fed_dec_2026 | 0.00 % | 34,826 | <= 0% / 1% | **PASS** | 0 rows; min 7.0 ms |
| latency | clock drift (slope of per-minute p5) | all | 0.8063 | 141 | |slope| <= 1.0 ms/h | **PASS** | ms/h, r² 0.54 |
| latency | trade recv - exch p50 | all | 11.4 ms | 11,932 | info | **PASS** | p99 90 ms |
| spot | quote update_id monotonic (violations) | all | 0 | 19,983 | = 0 | **PASS** | 11:39:15–11:55:20 UTC |
| spot | feed reconnect markers | all | 0 | 19,983 | info | **PASS** |  |
| spot | quote gaps > 5 s | all | 0 | 19,983 | = 0 | **PASS** | max gap 1.1 s |
| spot | trade_id holes | all | 0 | 4,782 | = 0 | **PASS** | 0 ids missing; 0 duplicates |
| spot | trade recv - event_ms p50 | all | 127.8 ms | 4,782 | p99 <= 1000 ms | **PASS** | p99 149 ms; Binance emit - trade time p50 0 ms |
| spot | Binance direction vs Up token (|mid-0.5| >= 0.1, |move| >= 0.05 %) | btc_15m | – | 0 | >= 80% (info below 3 windows) | **SKIP** | 1 windows with a Binance open |
| xfeed | Binance vs Polymarket lead (1 s xcorr) | btc_15m | +1 s | 847 | lag in [0, 15] s, corr >= 0.05 | **PASS** | max corr 0.346 at lag +1 s (corr at 0: 0.177); 847 overlapping seconds, 11 windows |

## Excluded

- 1 clean windows with no verifying snapshot (strict length 0): [('btc_15m', Decimal('1'))]
- 580 of 11,932 trades not judged against the book (reference instant outside a clean window, or book not valid)

Strict clean windows (trimmed to the last verified instant) are in `clean_windows_strict.parquet`; analyses should read that file, not `clean_windows`.

## Details

### Raw book_events per hour

| hour_utc | rows | tokens | deltas | snap_ws | snap_rest | reconnects | flag |
|---|---|---|---|---|---|---|---|
| 11-07 09:00 | 1,012,160 | 84 | 846,478 | 150,018 | 15,664 | 0 |  |
| 11-07 10:00 | 3,386,977 | 90 | 2,134,535 | 1,218,441 | 34,001 | 0 |  |
| 11-07 11:00 | 1,224,337 | 90 | 787,526 | 422,084 | 14,726 | 1 |  |

### Clean windows by end reason

| end_reason | n | loose_h | strict_h | n_unverified |
|---|---|---|---|---|
| end | 88 | 30.56 | 24.22 | 0 |
| reconnect | 94 | 88.74 | 86.91 | 0 |
| ws_gap | 84 | 63.77 | 57.02 | 1 |

### Spread inside clean windows (valid rows)

| family | n_valid_rows | spread_p50 | spread_p99 | spread_max |
|---|---|---|---|---|
| btc_15m | 3,169,641 | 0.01 | 0.03 | 0.45 |
| btc_daily_ladder | 1,266,881 | 0.023 | 0.055 | 0.095 |
| fed_dec_2026 | 44,283 | 0.01 | 0.01 | 0.01 |

### Tokens with the most crossed time

| asset_id | crossed_s | n | first_ts |
|---|---|---|---|
| 167470814756 | 0.0 | 113 | 10:15:13 |
| 191378301368 | 0.0 | 113 | 10:15:13 |
| 551788593462 | 0.0 | 85 | 11:44:54 |
| 557990255913 | 0.0 | 85 | 11:44:54 |
| 943559924400 | 0.0 | 55 | 09:58:22 |

### Delta latency per hour (ms)

| hour_utc | n | p5 | p50 | p95 | p99 | neg |
|---|---|---|---|---|---|---|
| 11-07 09:00 | 846,478 | 8.1 | 10.1 | 16.2 | 100.5 | 0 |
| 11-07 10:00 | 2,134,535 | 9.6 | 10.7 | 24.7 | 110.4 | 0 |
| 11-07 11:00 | 787,526 | 10.6 | 11.8 | 34.4 | 111.7 | 0 |

### Cross-correlation of 1 s returns (lag > 0: Binance leads)

| lag_s | corr | n |
|---|---|---|
| -10 | 0.03 | 829 |
| -9 | -0.0281 | 831 |
| -8 | 0.0301 | 833 |
| -7 | -0.0245 | 835 |
| -6 | 0.0162 | 837 |
| -5 | 0.0604 | 839 |
| -4 | -0.0313 | 841 |
| -3 | 0.035 | 843 |
| -2 | -0.0055 | 845 |
| -1 | 0.0291 | 847 |
| 0 | 0.1769 | 849 |
| 1 | 0.3455 | 847 |
| 2 | 0.0141 | 845 |
| 3 | -0.006 | 843 |
| 4 | 0.0049 | 841 |
| 5 | 0.0057 | 839 |
| 6 | -0.0304 | 837 |
| 7 | -0.0261 | 835 |
| 8 | -0.0408 | 833 |
| 9 | -0.1425 | 831 |
| 10 | 0.0793 | 829 |

## Thresholds

```
{
 "coverage_strict_pass": 0.85,
 "coverage_strict_warn": 0.7,
 "coverage_strict_pass_btc_15m": 0.6,
 "coverage_strict_warn_btc_15m": 0.4,
 "thin_hour_frac": 0.2,
 "crossed_seconds_warn": 60.0,
 "crossed_seconds_fail": 600.0,
 "empty_side_time_warn": 0.01,
 "empty_side_time_fail": 0.05,
 "spread_outlier": 0.2,
 "spread_outlier_time_warn": 0.01,
 "live_window_tail_s": 180.0,
 "mirror_px_pass": 0.999,
 "mirror_px_warn": 0.99,
 "mirror_kept_share_warn": 0.05,
 "trade_in_spread_pass": 0.9,
 "trade_in_spread_warn": 0.8,
 "trade_side_pass": 0.99,
 "trade_side_warn": 0.95,
 "trade_wrong_side_warn": 0.005,
 "trade_wrong_side_fail": 0.02,
 "trade_matched_pass": 0.95,
 "trade_matched_warn": 0.8,
 "latency_p50_warn_ms": 100.0,
 "latency_p50_fail_ms": 1000.0,
 "latency_p99_warn_ms": 2000.0,
 "latency_drift_warn_ms_per_h": 1.0,
 "latency_negative_share_warn": 0.0,
 "latency_negative_share_fail": 0.01,
 "spot_gap_s": 5.0,
 "spot_latency_p99_warn_ms": 1000.0,
 "spot_direction_pass": 0.8,
 "xcorr_lag_pass_min_s": 0,
 "xcorr_lag_pass_max_s": 15,
 "xcorr_min_corr": 0.05
}
```
