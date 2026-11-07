# Data quality — 2026-11-06

Data root `/Users/macbook/Desktop/POLY MM/data`; window 2026-11-06 13:10 → 2026-11-07 00:25 UTC. Replay ran in 453 s (53,879,011 rows, 8 reconnects, 81 WS mismatches, 0 REST resyncs). Checks took 127 s (coverage 2 s, book 75 s, mirror 1 s, trades 44 s, latency 6 s, spot 0 s, xfeed 0 s).

**2 FAIL · 2 WARN · 63 PASS · 2 SKIP**

| section | check | family | value | n | threshold | status | note |
|---|---|---|---|---|---|---|---|
| coverage | clean share (loose) | all | 99.94 % | 845 | info | **PASS** | 764.3 h of 764.8 h subscribed |
| coverage | clean share (strict) | all | 92.82 % | 845 | >= 85% / 70% | **PASS** | 709.9 h; 84 windows never verified; 0 tokens without a window |
| coverage | clean share (loose) | btc_15m | 99.94 % | 169 | info | **PASS** | 70.2 h of 70.3 h subscribed |
| coverage | clean share (strict) | btc_15m | 92.42 % | 169 | >= 60% / 40% | **PASS** | 64.9 h; 6 windows never verified; 0 tokens without a window |
| coverage | clean share (loose) | btc_daily_ladder | 99.94 % | 582 | info | **PASS** | 581.7 h of 582.1 h subscribed |
| coverage | clean share (strict) | btc_daily_ladder | 92.36 % | 582 | >= 85% / 70% | **PASS** | 537.6 h; 72 windows never verified; 0 tokens without a window |
| coverage | clean share (loose) | fed_dec_2026 | 99.95 % | 94 | info | **PASS** | 112.4 h of 112.4 h subscribed |
| coverage | clean share (strict) | fed_dec_2026 | 95.45 % | 94 | >= 85% / 70% | **PASS** | 107.3 h; 6 windows never verified; 0 tokens without a window |
| coverage | windows ending by end | all | 70 | 70 | info | **PASS** | 77.2 h loose, 71.4 h strict, 0 unverified |
| coverage | windows ending by reconnect | all | 694 | 694 | info | **PASS** | 619.1 h loose, 575.1 h strict, 84 unverified |
| coverage | windows ending by ws_gap | all | 81 | 81 | info | **PASS** | 68.0 h loose, 63.4 h strict, 0 unverified |
| coverage | thin hours | all | 0 | 10 | < 20% of median hour (4,633,418 rows) | **PASS** | none |
| book | crossed book (bid >= ask) | all | 1.7 s | 6,362 | <= 60 s / 600 s | **PASS** | 6,362 rows, 0.000 % of window time |
| book | one side empty (time share) | all | 15.67 % | 390,959 | info | **PASS** | 390,959 rows, 431260 s; structural for far strikes / unstarted windows |
| book | spread > 0.20 (time share) | all | 0.00 % | 9,039 | <= 1% | **PASS** | 9,039 rows, 95 s |
| book | crossed book (bid >= ask) | btc_15m | 1.7 s | 5,794 | <= 60 s / 600 s | **PASS** | 5,794 rows, 0.001 % of window time |
| book | one side empty (time share) | btc_15m | 8.41 % | 298,760 | info | **PASS** | 298,760 rows, 21264 s; structural for far strikes / unstarted windows |
| book | spread > 0.20 (time share) | btc_15m | 0.00 % | 2,261 | <= 1% | **PASS** | 2,261 rows, 4 s |
| book | crossed book (bid >= ask) | btc_daily_ladder | 0.1 s | 502 | <= 60 s / 600 s | **PASS** | 502 rows, 0.000 % of window time |
| book | one side empty (time share) | btc_daily_ladder | 19.58 % | 92,199 | info | **PASS** | 92,199 rows, 409996 s; structural for far strikes / unstarted windows |
| book | spread > 0.20 (time share) | btc_daily_ladder | 0.00 % | 6,778 | <= 1% | **PASS** | 6,778 rows, 91 s |
| book | crossed book (bid >= ask) | fed_dec_2026 | 0.0 s | 66 | <= 60 s / 600 s | **PASS** | 66 rows, 0.000 % of window time |
| book | one side empty (time share) | fed_dec_2026 | 0.00 % | 0 | info | **PASS** | 0 rows, 0 s; structural for far strikes / unstarted windows |
| book | spread > 0.20 (time share) | fed_dec_2026 | 0.00 % | 0 | <= 1% | **PASS** | 0 rows, 0 s |
| book | live 15 m window (first 720 s): one side empty (time share) | btc_15m | 0.59 % | 11,315,909 | <= 1% / 5% | **PASS** | 192 s of 32354 s in 46 live Up-token windows; crossed 0.7 s |
| mirror | pre-A1: YES/NO prices mirrored (1 s grid) | all | 100.00 % | 1,161,771 | >= 99.9% / 99% | **PASS** | until 00:25:00 UTC |
| mirror | pre-A1: prices and level-1 sizes mirrored | all | 100.00 % | 1,161,771 | info | **PASS** |  |
| mirror | pre-A1: YES/NO prices mirrored (1 s grid) | btc_15m | 100.00 % | 115,858 | >= 99.9% / 99% | **PASS** | until 00:25:00 UTC |
| mirror | pre-A1: prices and level-1 sizes mirrored | btc_15m | 100.00 % | 115,858 | info | **PASS** |  |
| mirror | pre-A1: YES/NO prices mirrored (1 s grid) | btc_daily_ladder | 100.00 % | 843,598 | >= 99.9% / 99% | **PASS** | until 00:25:00 UTC |
| mirror | pre-A1: prices and level-1 sizes mirrored | btc_daily_ladder | 100.00 % | 843,598 | info | **PASS** |  |
| mirror | pre-A1: YES/NO prices mirrored (1 s grid) | fed_dec_2026 | 100.00 % | 202,315 | >= 99.9% / 99% | **PASS** | until 00:25:00 UTC |
| mirror | pre-A1: prices and level-1 sizes mirrored | fed_dec_2026 | 100.00 % | 202,315 | info | **PASS** |  |
| trades | price inside/at the spread (pre-trade book) | all | 96.07 % | 56,959 | >= 90% / 80% | **PASS** | 61,053 trades, 4,094 not judged (outside windows / book not valid); 56,329 taker BUY; 3.9 % beyond the touch = multi-level sweeps |
| trades | side consistent (BUY >= ask, SELL <= bid) | all | 99.22 % | 56,959 | >= 99% / 95% | **PASS** |  |
| trades | wrong side (BUY < bid or SELL > ask) | all | 0.06 % | 56,959 | <= 0.5% / 2% | **PASS** | 33 trades |
| trades | trade has a size decrease at its price within 1 s | all | 99.77 % | 61,053 | >= 95% / 80% | **PASS** | 143 unmatched of 61,053 in windows |
| trades | price inside/at the spread (pre-trade book) | btc_15m | 96.20 % | 53,989 | >= 90% / 80% | **PASS** | 57,774 trades, 3,785 not judged (outside windows / book not valid); 53,812 taker BUY; 3.8 % beyond the touch = multi-level sweeps |
| trades | side consistent (BUY >= ask, SELL <= bid) | btc_15m | 99.37 % | 53,989 | >= 99% / 95% | **PASS** |  |
| trades | wrong side (BUY < bid or SELL > ask) | btc_15m | 0.05 % | 53,989 | <= 0.5% / 2% | **PASS** | 25 trades |
| trades | trade has a size decrease at its price within 1 s | btc_15m | 99.94 % | 57,774 | >= 95% / 80% | **PASS** | 35 unmatched of 57,774 in windows |
| trades | price inside/at the spread (pre-trade book) | btc_daily_ladder | 91.10 % | 2,000 | >= 90% / 80% | **PASS** | 2,277 trades, 277 not judged (outside windows / book not valid); 1,749 taker BUY; 8.5 % beyond the touch = multi-level sweeps |
| trades | side consistent (BUY >= ask, SELL <= bid) | btc_daily_ladder | 95.10 % | 2,000 | >= 99% / 95% | **WARN** |  |
| trades | wrong side (BUY < bid or SELL > ask) | btc_daily_ladder | 0.40 % | 2,000 | <= 0.5% / 2% | **PASS** | 8 trades |
| trades | trade has a size decrease at its price within 1 s | btc_daily_ladder | 95.61 % | 2,277 | >= 95% / 80% | **PASS** | 100 unmatched of 2,277 in windows |
| trades | price inside/at the spread (pre-trade book) | fed_dec_2026 | 99.07 % | 970 | >= 90% / 80% | **PASS** | 1,002 trades, 32 not judged (outside windows / book not valid); 768 taker BUY; 0.9 % beyond the touch = multi-level sweeps |
| trades | side consistent (BUY >= ask, SELL <= bid) | fed_dec_2026 | 99.59 % | 970 | >= 99% / 95% | **PASS** |  |
| trades | wrong side (BUY < bid or SELL > ask) | fed_dec_2026 | 0.00 % | 970 | <= 0.5% / 2% | **PASS** | 0 trades |
| trades | trade has a size decrease at its price within 1 s | fed_dec_2026 | 99.20 % | 1,002 | >= 95% / 80% | **PASS** | 8 unmatched of 1,002 in windows |
| trades | fill rows per labelled trade (replay over-labelling) | all | 10.88 | 1,167,581 | <= 1.5 / 3 | **FAIL** | 107,280 trades label 1,167,581 fill rows = 5.7 % of 20,472,731 size decreases; 7.9 % of fill rows shrink by exactly the trade size |
| trades | fill rows per labelled trade (replay over-labelling) | btc_15m | 11.44 | 1,158,249 | <= 1.5 / 3 | **FAIL** | 101,244 trades label 1,158,249 fill rows = 8.7 % of 13,266,889 size decreases; 7.6 % of fill rows shrink by exactly the trade size |
| trades | fill rows per labelled trade (replay over-labelling) | btc_daily_ladder | 1.587 | 6,752 | <= 1.5 / 3 | **WARN** | 4,254 trades label 6,752 fill rows = 0.1 % of 7,012,716 size decreases; 41.4 % of fill rows shrink by exactly the trade size |
| trades | fill rows per labelled trade (replay over-labelling) | fed_dec_2026 | 1.448 | 2,580 | <= 1.5 / 3 | **PASS** | 1,782 trades label 2,580 fill rows = 1.3 % of 193,126 size decreases; 44.7 % of fill rows shrink by exactly the trade size |
| latency | delta recv - exch p50 | all | 16.6 ms | 42,187,630 | <= 100 / 1000 ms | **PASS** | p95 155 ms, p99 554 ms |
| latency | delta recv - exch p99 | all | 554.5 ms | 42,187,630 | <= 2000 ms | **PASS** |  |
| latency | negative latency (recv before exch) | all | 0.00 % | 42,187,630 | <= 0% / 1% | **PASS** | 0 rows; min 4.1 ms |
| latency | delta recv - exch p50 | btc_15m | 17.4 ms | 26,852,834 | <= 100 / 1000 ms | **PASS** | p95 158 ms, p99 551 ms |
| latency | delta recv - exch p99 | btc_15m | 550.9 ms | 26,852,834 | <= 2000 ms | **PASS** |  |
| latency | negative latency (recv before exch) | btc_15m | 0.00 % | 26,852,834 | <= 0% / 1% | **PASS** | 0 rows; min 4.3 ms |
| latency | delta recv - exch p50 | btc_daily_ladder | 15.1 ms | 14,916,644 | <= 100 / 1000 ms | **PASS** | p95 148 ms, p99 567 ms |
| latency | delta recv - exch p99 | btc_daily_ladder | 566.9 ms | 14,916,644 | <= 2000 ms | **PASS** |  |
| latency | negative latency (recv before exch) | btc_daily_ladder | 0.00 % | 14,916,644 | <= 0% / 1% | **PASS** | 0 rows; min 4.1 ms |
| latency | delta recv - exch p50 | fed_dec_2026 | 15.2 ms | 418,152 | <= 100 / 1000 ms | **PASS** | p95 134 ms, p99 418 ms |
| latency | delta recv - exch p99 | fed_dec_2026 | 418.0 ms | 418,152 | <= 2000 ms | **PASS** |  |
| latency | negative latency (recv before exch) | fed_dec_2026 | 0.00 % | 418,152 | <= 0% / 1% | **PASS** | 0 rows; min 4.6 ms |
| latency | clock drift (slope of per-minute p5) | all | 0.1464 | 675 | |slope| <= 1.0 ms/h | **PASS** | ms/h, r² 0.07 |
| latency | trade recv - exch p50 | all | 16.5 ms | 61,053 | info | **PASS** | p99 522 ms |
| spot | Binance feed | all | – | 0 |  | **SKIP** | no spot tables in this dataset |
| xfeed | Binance vs Polymarket lead (1 s xcorr) | btc_15m | – | 0 |  | **SKIP** | needs spot_quotes and depth_1s |

## Excluded

- 84 clean windows with no verifying snapshot (strict length 0): [('btc_15m', Decimal('6')), ('btc_daily_ladder', Decimal('72')), ('fed_dec_2026', Decimal('6'))]
- 4,094 of 61,053 trades not judged against the book (reference instant outside a clean window, or book not valid)

Strict clean windows (trimmed to the last verified instant) are in `clean_windows_strict.parquet`; analyses should read that file, not `clean_windows`.

## Details

### Raw book_events per hour

| hour_utc | rows | tokens | deltas | snap_ws | snap_rest | reconnects | flag |
|---|---|---|---|---|---|---|---|
| 11-06 13:00 | 4,659,519 | 89 | 3,884,840 | 744,016 | 30,660 | 3 |  |
| 11-06 14:00 | 6,088,286 | 90 | 5,150,296 | 896,584 | 41,406 | 0 |  |
| 11-06 15:00 | 7,941,804 | 91 | 6,963,972 | 944,592 | 33,238 | 2 |  |
| 11-06 16:00 | 5,279,894 | 90 | 4,723,046 | 523,075 | 33,773 | 0 |  |
| 11-06 17:00 | 4,796,554 | 91 | 4,197,820 | 571,854 | 26,879 | 1 |  |
| 11-06 18:00 | 4,470,282 | 68 | 3,775,592 | 666,603 | 28,087 | 0 |  |
| 11-06 19:00 | 4,012,735 | 68 | 3,142,058 | 844,534 | 26,143 | 0 |  |
| 11-06 20:00 | 5,324,244 | 69 | 3,306,442 | 1,991,394 | 26,407 | 1 |  |
| 11-06 21:00 | 3,679,927 | 68 | 2,376,100 | 1,278,463 | 25,364 | 0 |  |
| 11-06 22:00 | 3,013,476 | 68 | 1,980,754 | 1,007,852 | 24,870 | 0 |  |
| 11-06 23:00 | 3,701,846 | 69 | 1,996,932 | 1,676,606 | 28,307 | 1 |  |
| 11-07 00:00 | 910,452 | 64 | 689,778 | 210,870 | 9,804 | 0 |  |

### Clean windows by end reason

| end_reason | n | loose_h | strict_h | n_unverified |
|---|---|---|---|---|
| end | 70 | 77.2 | 71.41 | 0 |
| reconnect | 694 | 619.1 | 575.1 | 84 |
| ws_gap | 81 | 68.02 | 63.35 | 0 |

### Spread inside clean windows (valid rows)

| family | n_valid_rows | spread_p50 | spread_p99 | spread_max |
|---|---|---|---|---|
| btc_15m | 26,664,426 | 0.01 | 0.03 | 0.79 |
| btc_daily_ladder | 14,832,685 | 0.018 | 0.069 | 0.678 |
| fed_dec_2026 | 420,876 | 0.01 | 0.02 | 0.08 |

### Tokens with the most crossed time

| asset_id | crossed_s | n | first_ts |
|---|---|---|---|
| 704940078278 | 0.2 | 60 | 22:14:31 |
| 953111349148 | 0.2 | 60 | 22:14:31 |
| 261814283737 | 0.1 | 30 | 22:44:39 |
| 110682526283 | 0.1 | 30 | 22:44:39 |
| 481948017849 | 0.0 | 205 | 22:56:58 |

### Delta latency per hour (ms)

| hour_utc | n | p5 | p50 | p95 | p99 | neg |
|---|---|---|---|---|---|---|
| 11-06 13:00 | 3,884,840 | 7.5 | 15.2 | 170.1 | 724.7 | 0 |
| 11-06 14:00 | 5,150,296 | 6.9 | 17.2 | 241.8 | 1272.3 | 0 |
| 11-06 15:00 | 6,963,972 | 8.3 | 13.9 | 118.0 | 313.0 | 0 |
| 11-06 16:00 | 4,723,046 | 7.7 | 14.0 | 206.2 | 821.9 | 0 |
| 11-06 17:00 | 4,197,820 | 10.9 | 17.7 | 112.2 | 320.9 | 0 |
| 11-06 18:00 | 3,775,592 | 12.7 | 20.5 | 189.1 | 4904.0 | 0 |
| 11-06 19:00 | 3,142,058 | 10.6 | 20.6 | 153.4 | 357.3 | 0 |
| 11-06 20:00 | 3,306,442 | 10.3 | 19.1 | 162.7 | 458.9 | 0 |
| 11-06 21:00 | 2,376,100 | 10.2 | 15.2 | 102.9 | 250.5 | 0 |
| 11-06 22:00 | 1,980,754 | 9.7 | 14.8 | 116.7 | 254.8 | 0 |
| 11-06 23:00 | 1,996,932 | 8.6 | 13.8 | 147.9 | 26475.0 | 0 |
| 11-07 00:00 | 689,778 | 7.8 | 11.0 | 93.8 | 183.7 | 0 |

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
