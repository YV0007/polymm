# Volatility (step 5): σ inputs for the fair-value model and the inventory-risk numbers

Date: 2026-11-07. Code: `src/polymm/vol.py`, CLI `polymm vol`, tests `tests/test_vol.py` (13 tests on synthetic GBM paths).
Outputs: `reports/2026-11-06/vol/` and `reports/2026-11-07/vol/` (tables as CSV, PNGs, `summary.md`, and the
σ(t) series `sigma_btc.parquet` that step 6 reads).

**Everything here is provisional.** The BTC side rests on three full days of Binance history (Wed 2026-11-04 to
Fri 11-06) plus one Saturday afternoon of our own feed (11-07 11:39 → 14:57 UTC); the Polymarket side on the
2026-11-06 laptop block (13:10 → 00:25 UTC) and the 11-07 VPS day (09:35 → 14:57 UTC), strict clean windows only.
Every number carries its n.

## 1. Which series feeds which number

| series | clock | used for |
|---|---|---|
| `backfill/spot_trades_hist` (every Binance BTC/USDT trade of a finished day) | exchange, µs | 1 s last-trade series and 1-min OHLC bars → realized σ, EWMA, hour-of-day profile, 15-min move distribution, forecast scores, 1-min jumps, signature plot |
| `spot/spot_trades` (live trades, days not yet in the history) | exchange, ms | stitched onto the history bars (history wins where both exist) |
| `spot/spot_quotes` (Binance bookTicker) | **ours**, ns | 1 s quote mid: noise check vs trades, 1 s moves ≥ 10 bp, and the only BTC series on the Polymarket clock → implied σ and the σ(t) series on VPS days |
| replay `tob` inside `clean_windows_strict` | ours, ns | last valid Polymarket mid per second (as-of) → x = logit(mid), σ_x, σ_p check, jump rates |

## 2. Formulas (as implemented)

- Log return r_t = ln(P_t/P_{t−1}). Realized σ per step: σ_step = sqrt( (1/n) Σ r_t² ) (not demeaned).
- Annualised with a 365-day year because BTC trades 24/7: σ_ann = σ_step · sqrt(31 536 000 / step_s); from 1-min
  returns σ_ann = σ_1m · sqrt(525 600).
- Horizon h: σ_h = σ_step · sqrt(h / step_s). The 15-minute horizon from an annual σ: σ_15m = σ_ann · sqrt(900 / 31 536 000) = σ_ann / 187.2;
  from a per-minute σ: σ_15m = σ_1m · √15.
- EWMA (RiskMetrics): σ²_t = λ σ²_{t−1} + (1 − λ) r_t², initialised with the mean r² of the first 1/(1−λ) returns; the
  value after return t is the forecast for t+1. A missing minute contributes r = 0 (variance decays, recursion not restarted).
- Deseasonalised EWMA: r̃_t = r_t / s_{h(t)} with s_h = sqrt( mean r² in UTC hour h / mean r² overall ) computed from the
  OTHER days (leave-one-day-out), σ̃² by the same recursion, forecast for a window in hour h: 15 · σ̃² · s_h².
- Parkinson σ² = (ln(H/L))² / (4 ln 2); Garman–Klass σ² = ½ (ln(H/L))² − (2 ln 2 − 1)(ln(C/O))², per 1-min bar.
- Forecast losses on the 15-min variance proxy y = (ln S_end/S_start)²: QLIKE = mean( y/σ̂² − ln(y/σ̂²) − 1 ),
  MSE = mean( (y − σ̂²)² ). For a perfect forecast QLIKE is not 0 but E[z − ln z − 1] with z ~ χ²₁, ≈ 1.27, because a single squared move is a noisy proxy.
- Signature plot: var_per_s(Δ) = Σ (non-overlapping Δ-second returns)² / (n·Δ). Flat in Δ for a diffusion; iid noise of
  variance η² adds 2η²/Δ.
- Polymarket: x = ln(p/(1−p)); σ_x(k) = std( x_{t+k} − x_t ) on the 1 s grid, both mids in [0.02, 0.98], same strict
  window; the first-order relation σ_p ≈ p(1−p) σ_x.
- Implied σ of a 15-minute market: mid_t = Φ( ln(S_t/S_0) / (σ √τ_t) ), so Φ⁻¹(mid_t) √τ_t = a + (1/σ) ln(S_t/S_0): one OLS
  per market window gives σ = 1/slope; the intercept a absorbs an offset between Binance's S_0 and the market's
  (Chainlink) reference price. Point-wise σ = ln(S_t/S_0) / (Φ⁻¹(mid_t) √τ) is also reported (only where |ln S_t/S_0| ≥ 1 bp and |mid − ½| ≥ 0.02).

## 3. BTC (Binance)

### 3.1 Realized σ by day (`btc_realized_by_day`)

| day | σ_1m (bp) | σ_ann from 1-min | σ_15m (bp) | σ_1s (bp) | σ_ann from 1 s | Parkinson 1-min (bp) | GK 1-min (bp) | n_1m |
|---|---|---|---|---|---|---|---|---|
| Wed 11-04 | 7.04 | 0.51 | 27.3 | 0.724 | 0.41 | 5.93 | 5.44 | 1,439 |
| Thu 11-05 | 7.55 | 0.55 | 29.2 | 0.825 | 0.46 | 6.57 | 6.16 | 1,440 |
| Fri 11-06 | 6.37 | 0.46 | 24.7 | 0.654 | 0.37 | 5.28 | 4.80 | 1,440 |
| Sat 11-07 (11:39–14:55 only) | 1.52 | 0.11 | 5.9 | 0.269 | 0.15 | 0.97 | 0.65 | 196 |
| all | 6.85 | 0.50 | 26.5 | 0.724 | 0.41 | 5.82 | 5.37 | 4,515 |

The 1 s estimate is lower than the 1-min one by the signature-plot factor (§3.4), the range estimators are lower
still (a 1-min range under-samples the path and the same short-horizon autocorrelation). The Saturday afternoon is
4–5× quieter than the weekdays: one more reason the hour/weekday profile matters.

### 3.2 Rolling windows and EWMA (`btc_rolling_ewma`, annualised; p50_15m = median σ per 15 min in bp)

| estimator | n | p10 | p50 | p90 | p50 σ_15m (bp) |
|---|---|---|---|---|---|
| rolling 5 min on 1 s returns | 270,954 | 0.12 | 0.26 | 0.58 | 13.7 |
| rolling 15 min on 1 s returns | 270,954 | 0.14 | 0.26 | 0.56 | 14.0 |
| rolling 60 min on 1 s returns | 270,954 | 0.16 | 0.27 | 0.60 | 14.7 |
| EWMA λ=0.94 on 1-min returns | 4,417 | 0.20 | 0.35 | 0.75 | 18.8 |
| EWMA λ=0.97 on 1-min returns | 4,417 | 0.21 | 0.37 | 0.76 | 19.5 |
| EWMA λ=0.99 on 1-min returns | 4,417 | 0.24 | 0.40 | 0.76 | 21.4 |
| rolling 5 / 15 / 60 min on 1-min returns | 4,417 | 0.13 / 0.17 / 0.21 | 0.32 / 0.34 / 0.36 | 0.75 / 0.74 / 0.74 | 16.9 / 18.0 / 19.3 |

Medians sit well below the full-sample 0.50 because the sample mean is driven by a few hours (the distribution of
σ is right-skewed: p90/p10 ≈ 3.5–4). Note that the 1 s-based rolling windows are ~25 % lower than the 1-min-based ones (§3.4).

### 3.3 Hour of day, UTC (`btc_by_hour_utc`; 180 minutes per hour over the three weekdays; 12:00 ET = 17 UTC)

| hour UTC | 0–5 | 6–11 | 12 | 13 | 14 | 15 | 16 | 17 | 18–22 | 23 |
|---|---|---|---|---|---|---|---|---|---|---|
| σ_1m (bp) | 7.3 · 5.6 · 7.0 · 6.0 · 5.7 (hour 0: no first return) | 3.8 · 5.1 · 4.2 · 4.3 · 3.5 · **3.0** | 5.6 | 7.1 | 11.3 | **16.5** | 9.8 | 6.6 | 6.4 · 5.4 · 5.4 · 4.1 · 5.1 | 8.3 |
| σ_15m (bp) | 22–28 | 12–20 | 21.7 | 27.3 | 43.7 | **64.0** | 37.8 | 25.7 | 16–25 | 32.2 |

A 5.5× range between the quietest hour (11 UTC, European midday) and the loudest (15 UTC = 10–11 ET: US open and
the 10:00 ET data releases). The ladder's 12:00 ET resolution falls in hour 17 (6.6 bp/min, average), right after the
loudest block, which is where the ladder strikes near the money get decided. Weekend vs weekday: the only weekend
data is the Saturday afternoon above (1.5 bp/min vs 3–11 bp/min for the same hours on weekdays); a proper weekday
profile needs more days and is the first thing to redo when the nightly backfill has a week.

### 3.4 Microstructure noise: signature plot (`btc_signature`, `btc_signature.png`)

| Δ (s) | 1 | 2 | 5 | 10 | 15 | 30 | 60 | 120 | 300 | 600 |
|---|---|---|---|---|---|---|---|---|---|---|
| last trade, var/s (bp²/s), 3 days | 0.545 | 0.602 | 0.684 | 0.742 | 0.754 | 0.799 | 0.817 | 0.829 | 0.805 | 0.799 |
| ratio to Δ=300 s | 0.68 | 0.75 | 0.85 | 0.92 | 0.94 | 0.99 | 1.02 | 1.03 | 1.00 | 0.99 |
| n (returns) | 259,197 | 129,597 | 51,837 | 25,917 | 17,277 | 8,637 | 4,317 | 2,157 | 861 | 429 |
| quote mid, var/s, Sat 11:39–14:57 | 0.024 | 0.024 | 0.026 | 0.028 | 0.030 | 0.031 | 0.036 | 0.036 | 0.037 | 0.023 |
| ratio to Δ=300 s | 0.64 | 0.65 | 0.70 | 0.75 | 0.82 | 0.84 | 0.97 | 0.97 | 1.00 | 0.63 (n=19) |

The curve **rises** with Δ instead of falling: there is no bid-ask-bounce noise in the Binance BTC/USDT 1 s series,
there is positive autocorrelation of 1 s returns (the price trends over a few seconds; one tick is 0.01 on 76,000, far
below the 1 s σ, so discreteness is not it). Consequences: (i) variance estimated from 1 s returns is ~32 % low
(σ 18 % low); the shortest sampling that is not biased is **30–60 s**; (ii) anything that needs σ at the 1 s scale
(the σ(t) series' rolling 15-min RV, implied-vs-realized) is reported from 1 s and from 60 s sampling and the report
says which. Separately, on the same Saturday span the 1 s σ from last trades is 0.270 bp vs 0.154 bp from the quote
mid (n = 11,749 s): trade prints bounce between bid and ask, the mid does not, so sub-minute work uses the quote mid.

### 3.5 15-minute moves (`btc_moves_15m_dist`; all quarter-hour boundaries with a price within 60 s of both ends)

| n windows | std (bp) | mean abs | p50 abs | p90 abs | p99 abs | max abs | share > 5 bp | > 10 bp | > 20 bp |
|---|---|---|---|---|---|---|---|---|---|
| 299 | 27.3 | 17.3 | 10.4 | 38.6 | 82.6 | 188 | 74.9 % | 52.5 % | 30.4 % |

Half of the windows move more than 10 bp, i.e. P(Up) is far from ½ for most of a window's life; the largest moves
(141 and 188 bp, hour 14–15 UTC) are 7σ events for a Gaussian with std 27 bp. By hour (`btc_moves_15m_by_hour`): rms
move 10–26 bp in most hours, 49 bp in hour 14 and 79 bp in hour 15 UTC.

### 3.6 Forecast quality at the 15-minute horizon (`btc_forecast_scores`; n = 299 windows, forecasts made at the window start)

| estimator | QLIKE ↓ | MSE (bp⁴) ↓ | mean forecast σ (bp) | realized std (bp) | var bias ratio | share \|move\| > 2σ̂ |
|---|---|---|---|---|---|---|
| hour-of-day profile (in-sample, reference only) | 1.610 | 6.90 M | 27.2 | 27.3 | 0.99 | 5.4 % |
| **EWMA λ=0.99 on deseasonalised returns, LOO hour profile** | **1.651** | 8.12 M | 29.6 | 27.3 | 1.17 | 6.7 % |
| EWMA λ=0.97 on deseasonalised returns, LOO hour profile | 1.700 | 7.74 M | 28.9 | 27.3 | 1.12 | 6.7 % |
| EWMA λ=0.97 | 1.826 | 7.86 M | 26.5 | 27.3 | 0.94 | 7.0 % |
| rolling 60 min RV | 1.880 | 8.18 M | 26.7 | 27.2 | 0.96 | 7.1 % |
| EWMA λ=0.94 | 1.901 | 7.69 M | 26.1 | 27.3 | 0.91 | 7.4 % |
| EWMA λ=0.99 | 1.928 | 8.13 M | 27.0 | 27.3 | 0.98 | 5.0 % |
| constant (in-sample) | 2.070 | 8.17 M | 26.5 | 27.3 | 0.94 | 4.0 % |
| rolling 15 min RV | 5.87 | 7.98 M | 26.6 | 27.3 | 0.95 | 8.4 % |
| rolling 5 min RV | 98,743 | 7.60 M | 24.5 | 27.3 | 0.80 | 17.1 % |

**Chosen for step 6: EWMA λ = 0.99 on hour-deseasonalised 1-min returns, re-seasonalised with the hour profile
(QLIKE 1.651, n = 299).** Why: the intraday pattern is the single largest predictable component (the in-sample
profile alone beats every EWMA), and the plain EWMAs lag it by their memory (λ=0.97 ≈ 33 min); deseasonalising lets
the EWMA track the level while the profile handles the time of day, and it wins out of sample with the profile
built from the other two days. The plain EWMA λ=0.97 is the fallback if the profile is not trusted (it is the best
non-seasonal estimator). Short rolling windows are not usable as forecasts: a 5-minute RV is often exactly zero
(QLIKE explodes). Caveats: the chosen estimator over-forecasts variance by 17 % here (a two-day profile is noisy),
all estimators leave 6–7 % of windows beyond 2σ̂ (Gaussian: 4.6 %), i.e. the tails are fatter than the σ says, and
299 windows from three weekdays cannot separate λ = 0.97 from 0.99 reliably. Re-score when the history has ≥ 2 weeks.

### 3.7 Jumps (`btc_jumps_*`)

- 1-min returns beyond 4σ (σ = EWMA λ=0.97 forecast made before the minute): **6, 6, 5 per day** on 11-04/05/06
  (1 on the Saturday afternoon). Largest: +71 bp (11-04 15:04 UTC), +54 bp (11-05 23:02), −39 bp (11-06 13:30), −39 bp
  (11-05 14:36), +36 bp (11-04 14:30). By hour: 8 of 17 in 12–15 UTC, 5 in 22–23 UTC, none in 16–21.
- 1 s moves ≥ 10 bp (last-trade series): 4, 10, 6 per day; max 59 bp (11-06), 18 bp (11-05), 14 bp (11-04). On the
  quote mid (Saturday, 11,749 s): none.

### 3.8 The σ(t) series (`reports/<date>/vol/sigma_btc.parquet`, one row per second of the UTC day)

| column | meaning |
|---|---|
| `sec`, `px`, `src`, `gap_s` | second (unix), price (quote mid where our feed has it within 5 s, else last trade; null if no price within 60 s), source ∈ {quote, trade_hist, trade_live}, seconds since the last observation |
| `sigma_ewma_1m`, `_ann`, `_15m` | plain EWMA of the chosen λ on 1-min returns, known from the end of each bar; per minute, annualised (·√525 600), per 15 min (·√15) |
| `sigma_dewma_1m`, `sigma_fc_1m`, `_ann`, `_15m` | deseasonalised EWMA, and the **chosen forecast** = deseasonalised EWMA × hour multiplier of the current hour (profile from all history days except the day itself) |
| `sigma_rv15_1s`, `_ann`, `_15m` | realized σ of the trailing 900 s from 1 s returns (≥ 450 valid returns), ~18 % low per §3.4 |
| `ewma_age_s`, `lambda` | seconds since the bar that last updated the EWMA (41,000 s on the 11-07 morning: no BTC data before 11:39), the λ used |

For step 6: use `sigma_fc_15m` (or `sigma_fc_ann` in Φ(d₂)) as the forecast, `sigma_rv15_15m` × 1.21 as the "what
just happened" realized number, and skip rows with `gap_s` > 60 or `ewma_age_s` > 600.

## 4. Polymarket (strict clean windows, outcome-0 tokens = Yes / Up; outcome 1 is the mirror)

Grid coverage (seconds with a valid as-of mid inside a strict window): 11-06 block btc_15m 116,565 s (48 tokens,
73 windows), ladder 813,315 s (29 tokens, 214 windows), fed 192,691 s (5 tokens, 44 windows); 11-07 VPS day btc_15m
55,188 s, ladder 414,044 s, fed 88,874 s. The 11-07 strict windows come from a rerun of `polymm quality` on the
full day pulled this afternoon (85.4 % strict coverage, 9.96 M rows; written to a scratch folder, the committed
`reports/2026-11-07/quality.md` still describes the 09:35–11:55 slice).

### 4.1 σ_x per family and horizon (`poly_sigma_x_family`; std of Δx, both mids in [0.02, 0.98])

| family | day | k = 1 s | k = 10 s | k = 60 s | per √s (1 / 10 / 60) | share Δx = 0 at 1 s / 10 s | n (10 s) |
|---|---|---|---|---|---|---|---|
| btc_15m | 11-06 | 0.056 | **0.185** | 0.397 | 0.056 / 0.059 / 0.051 | 88 % / 72 % | 108,698 |
| btc_15m | 11-07 | 0.052 | **0.191** | 0.411 | 0.052 / 0.060 / 0.053 | 93 % / 75 % | 51,613 |
| btc_daily_ladder | 11-06 | 0.020 | **0.043** | 0.095 | 0.020 / 0.014 / 0.012 | 66 % / 39 % | 256,068 |
| btc_daily_ladder | 11-07 | 0.007 | **0.014** | 0.030 | 0.007 / 0.004 / 0.004 | 92 % / 83 % | 170,346 |
| fed_dec_2026 | 11-06 | 0.0017 | **0.0044** | 0.0095 | 0.0017 / 0.0014 / 0.0012 | 99.9 % / 99.5 % | 77,260 |
| fed_dec_2026 | 11-07 | 0 | **0** | 0 | – | 100 % | 35,262 |

btc_15m scales like √k (a diffusion in x at 0.05–0.06 per √s, i.e. 0.19 log-odds per 10 s, which at p = ½ is
σ_p ≈ 0.047 = almost 5 ticks per 10 s). The ladder is sub-diffusive from 1 s to 10 s (1 s changes partly revert:
quote flicker at the 0.001 tick) and then diffusive; its level is 4× below btc_15m on the Friday and 14× on the
Saturday. The Fed market's mid did not change once in 35,000 strict seconds on the Saturday and moved on 0.08 % of
seconds on the Friday: σ_x ≈ 0.004 per 10 s, two orders of magnitude below btc_15m.

### 4.2 btc_15m by minute within the window and by |mid − ½| (`poly_btc15_by_minute`, `poly_btc15_by_edge`; k = 10 s, 11-06 block, ~2,700 s per minute bucket)

| minute | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| σ_x(10 s) | 0.18 | 0.18 | 0.16 | 0.18 | 0.19 | 0.22 | 0.25 | 0.27 | 0.26 | 0.31 | 0.35 | 0.39 | 0.50 | 0.85 | 1.44 |
| n | 2,690 | 2,691 | 2,700 | 2,700 | 2,686 | 2,700 | 2,678 | 2,699 | 2,599 | 2,513 | 2,444 | 2,129 | 1,752 | 1,149 | 228 |

| \|mid − ½\| | 0–0.1 | 0.1–0.2 | 0.2–0.3 | 0.3–0.4 | 0.4–0.5 |
|---|---|---|---|---|---|
| σ_x(10 s) | 0.26 | 0.32 | 0.35 | 0.37 | 0.45 |
| n | 11,805 | 7,751 | 6,257 | 5,587 | 2,958 |

σ_x doubles from minute 0 to minute 11 and is 8× higher in the last minute (the 11-07 day shows the same shape:
0.15 → 0.95), exactly what Φ(ln(S_t/S_0)/(σ√τ)) says: dx/dS ∝ 1/√τ. Before the window starts (74,255 s) σ_x(10 s) is
0.018, the mid of an unstarted window does not follow BTC. For the inventory term this means the same q costs 8×
more variance in minute 14 than in minute 0: flatten before the last minutes, not at them.

### 4.3 Ladder by hours to 12:00 ET and by strike distance (`poly_ladder_*`; k = 10 s)

| hours to resolution | 0–1 h | 1–3 h | 3–6 h | 12–24 h | 24–48 h | 48–96 h |
|---|---|---|---|---|---|---|
| σ_x(10 s), 11-06 | **0.199** | 0.069 | 0.043 | 0.025 | 0.032 | 0.038 |
| n | 3,616 | 17,094 | 8,171 | 51,547 | 123,598 | 52,042 |
| σ_x(10 s), 11-07 (Sat) | – | 0.034 | 0.027 | 0.024 (6–12 h) | 0.017 | 0.007 |

| ln(K/S) | < −5 % | −5…−2.5 % | −2.5…−1 % | −1…0 % | 0…1 % | 1…2.5 % | 2.5…5 % | > 5 % |
|---|---|---|---|---|---|---|---|---|
| σ_x(10 s), 11-06 | 0.045 | 0.034 | 0.037 | **0.077** | **0.048** | 0.076 | 0.038 | 0.052 |
| n | 10,922 | 33,747 | 46,788 | 7,525 | 78,854 | 952 | 67,471 | 5,538 |

Crossed: strikes within 1 % of spot in the last hour have σ_x(10 s) = 0.199 (n = 3,616), the same strikes 12–48 h out
0.021–0.024. Far strikes (|ln K/S| > 2.5 %) sit at 0.03–0.05 at every horizon with 50–70 % of 10 s intervals showing
no change: that is tick flicker on a book nobody is repricing, not information.

### 4.4 σ_p ≈ p(1−p) σ_x (`poly_sigma_p_check_all`, k = 10 s, 11-06 block)

| p bin | 0.01–0.05 | 0.05–0.10 | 0.10–0.20 | 0.20–0.30 | 0.30–0.40 | 0.40–0.50 | 0.50–0.60 | 0.60–0.70 | 0.70–0.80 | 0.80–0.90 | 0.90–0.95 | 0.95–0.99 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| std(Δp)/std(Δx) ÷ p̄(1−p̄) | 1.36 | 0.98 | 1.09 | 1.04 | 0.92 | 0.93 | 0.92 | 0.86 | 1.01 | 0.98 | 1.32 | 3.17 |
| n | 108,280 | 23,198 | 11,392 | 44,014 | 17,651 | 117,388 | 39,051 | 4,857 | 42,304 | 26,498 | 31,733 | 106,091 |

Holds within ±10 % for p in [0.05, 0.90] (and per family, `poly_sigma_p_check_family`). It breaks at the edges because
a 1 ¢ tick is Δx = 0.5 at p = 0.98: price moves are tick-sized in p and therefore huge in x, so near 0/1 the right
variance measure is σ_p directly. Practical rule: work in x for 0.05 ≤ p ≤ 0.95, in p outside.

### 4.5 Jumps in the mid (`poly_jumps_x`; per hour of strict grid time)

| family | day | grid h | \|Δx\| ≥ 0.2 / 0.5 / 1.0 per h (mid in 2–98 %) | \|Δp\| ≥ 0.02 / 0.05 / 0.10 per h (all mids) |
|---|---|---|---|---|
| btc_15m | 11-06 | 32.4 | 57.8 / 6.0 / 0.89 | 165 / 23.6 / 3.5 |
| btc_15m | 11-07 | 15.3 | 43.9 / 7.3 / 1.25 | 94.8 / 20.6 / 5.4 |
| btc_daily_ladder | 11-06 | 225.9 | 3.0 / 0.31 / 0.056 | 2.0 / 0.45 / 0.14 |
| btc_daily_ladder | 11-07 | 115.0 | 0.23 / 0 / 0 | 0.63 / 0.009 / 0 |
| fed_dec_2026 | 11-06 | 53.5 | 0.047 / 0 / 0 | 0.056 / 0 / 0 |
| fed_dec_2026 | 11-07 | 24.7 | 0 / 0 / 0 | 0 / 0 / 0 |

A 5 ¢ one-second move happens 20–24 times per hour in a live btc_15m window and the rate climbs through the window
(`poly_btc15_jumps_by_minute`: 18–27 per minute-bucket in minutes 0–6, 65–83 in minutes 9–12, 150 in minute 13, of
~2,700 s each). For the cancel rule λ(t)·E[(|J|−h)⁺]·Q > ρ this is the λ(t) input: ~0.4 per minute early, ~1 per
minute from minute 9, ~3 per minute in minute 13. The ladder sees a 5 ¢ second-move about once per 2 hours and the
Fed market never.

### 4.6 Implied σ of the 15-minute market vs realized (`poly_implied_15m`, `btc15m_implied_vs_realized.png`; Saturday 11-07 11:45–14:45 UTC, provisional)

13 live windows with Binance quotes on our clock; 10 admit a fit (in 3 the spot never left ±1 bp of its open, so the
mid carries no information about σ: 12:15, 14:00, 14:15).

| window (UTC) | 11:45 | 12:00 | 12:30 | 12:45 | 13:00 | 13:15 | 13:30 | 13:45 | 14:30 | 14:45 |
|---|---|---|---|---|---|---|---|---|---|---|
| implied σ_15m (bp), window fit | 6.0 | 6.1 | 9.0 | 7.8 | 5.5 | 8.2 | 4.6 | 7.6 | 9.6 | 5.6 |
| r² of the fit / n | 0.54 / 725 | 0.81 / 751 | 0.96 / 469 | 0.95 / 608 | 0.95 / 741 | 0.74 / 738 | 0.82 / 685 | 0.87 / 791 | 0.48 / 809 | 0.92 / 501 |
| realized σ_15m (bp), 60 s returns | 4.8 | 6.4 | 14.5 | 5.6 | 4.9 | 6.1 | 2.5 | 5.6 | 4.3 | 4.9 |
| realized σ_15m (bp), 1 s returns | 4.1 | 4.9 | 10.3 | 3.5 | 3.9 | 5.3 | 1.8 | 4.3 | 4.8 | 3.6 |
| 15-min move (bp) | −3.7 | +2.2 | −21.3 | +7.2 | −7.3 | +7.6 | +5.2 | −0.7 | +2.4 | −0.6 |

Medians over the 10 windows: implied 6.9 bp per 15 min (0.13 annualised), realized 5.2 bp (60 s) / 4.2 bp (1 s);
**implied / realized = 1.29 (60 s sampling), 1.55 (1 s sampling); implied > realized in 8 of 10 windows.** So on
this Saturday afternoon the market priced roughly 30 % more σ than BTC then realized, consistent with a maker
premium or with makers using a weekday-level σ: against the history-based forecast at each window start (13 bp,
weekday level, EWMA age ≥ 2 h) the implied is 0.70×. The fit intercepts run from −6 to +10 (units of Φ⁻¹·√s): the
market's reference price is not the Binance mid at the window start, which the point-wise σ cannot absorb (its
median, 5.7 bp, agrees with the fits only because errors cancel). Whenever BTC was ≥ 3 bp from its open, the mid's side
agreed with Binance's direction in 99.9 % of seconds (n = 3,393): the market is never on the wrong side, only
differently scaled. Ten quiet Saturday windows: this number is a first look, not a result; the question "more or less
vol than realized" needs weekday windows and the Chainlink reference price.

## 5. Caveats and follow-ups

- Three weekdays of history: the hour profile, the λ choice and the jump rates are all first estimates; rerun
  `polymm vol` as the nightly backfill adds days (the chosen estimator is re-scored every run).
- The VPS recorder restarted around 14:48 UTC on 11-07 (health `uptime_s` back to 12 s in hour 14, 2 reconnects on the
  day); the gap is excluded by the strict windows but worth a look by the recorder owner.
- 11-07 Binance history is not published until the next UTC morning, so the 11-07 σ series uses live trades (from
  11:39) stitched to the three-day history; `ewma_age_s` flags the stale morning.
- The implied-σ comparison uses Binance's mid as S_0; the market resolves on Chainlink's price at the window start.
  The intercept shows the two differ; a Chainlink reference feed (or at least the resolution prices from
  `resolutions`) would remove the ambiguity.
- The ladder strike-distance table on 11-07 uses live Binance prices only from 11:39; before that the distance is unknown and those seconds are excluded.
- Not changed: `replay.py`, `quality.py`, `book.py`, the recorder modules, `CLAUDE.md`, `docs/DECISIONS.md`. Added
  `numpy` (runtime) and `matplotlib` (dev group) to `pyproject.toml`.
