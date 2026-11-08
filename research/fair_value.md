# Fair value v1 (stage 3, step 6)

Built 2026-11-08 by `polymm fair 2026-11-06 2026-11-07 2026-11-08` (code `src/polymm/fair/`, decision D-023).
Held-out days 11-07 and 11-08, each scored with parameters fitted only on the days before it.
Machine-generated tables: `reports/fair/summary.md`. Plots: `research/fair_value/`.

## 1. Result in one paragraph

The fair value that wins is **the book's level plus the BTC model's fresh news**, not a plain average of the two.
The BTC digital model has real lead information in its *changes*: after it moves at least 0.02 in log-odds within one second, and the Polymarket mid then moves within 5 s, the mid moves the same way 72 % of the time.
But its *level* sits a median 0.35 log-odds away from the market, so used directly it is worse than the mid on every metric.
Anchoring the model to the book's level and blending it with Stoikov's microprice gives a fair value that beats the mid and the microprice at 30 to 60 s in both BTC families and has the best Brier score.
The gains are small, which is normal for a 1-cent-tick market: about 1.5 % of the mid's 30-second squared error in btc_15m, and about 3 % in minutes 10 to 12, where BTC sensitivity rises.

## 2. The formula that is used

Log-odds $x = \ln \frac{p}{1-p}$ throughout.

$$
x^{\text{fair}}_t = x^{\text{book}}_t + w_t \left( x^{\text{anch}}_t - x^{\text{book}}_t \right)
$$

**Book.** Stoikov's empirical microprice: the mid plus the average next-5-second log-odds mid move for the current imbalance and spread bucket, fitted on earlier days and shrunk by $n/(n+200)$:

$$
x^{\text{book}}_t = x^{\text{mid}}_t + \mathbb{E}\left[ x^{\text{mid}}_{t+5} - x^{\text{mid}}_t \mid I_t, \text{spread}_t \right], \qquad I_t = \frac{Q_{\text{bid}}}{Q_{\text{bid}} + Q_{\text{ask}}}
$$

**Model.** The BTC digital option, with $d_t$, $\tau_{\text{eff}}$ and the Chainlink gap $b$ as in the plan:

$$
x^{\text{model}}_t = \ln \frac{\Phi(d_t)}{1 - \Phi(d_t)}, \qquad d_t = \frac{\ln\left( \mathbb{E}[A_T \mid \mathcal{F}_t] \right) - b - \ln K}{\sigma_t \sqrt{\tau_{\text{eff}}}}
$$

In the last minute of a 15-minute window, part of the 60-second settlement average is already known:

$$
\mathbb{E}[A_T \mid \mathcal{F}_t] = \frac{(60 - \tau)\, \bar{S}_{[T-60,\, t]} + \tau S_t}{60}
$$

**Anchor.** The model takes its level from the book through a slow offset, a random-walk-offset Kalman filter in its simplest form:

$$
x^{\text{anch}}_t = x^{\text{model}}_t + \text{EWMA}_h\left( x^{\text{book}} - x^{\text{model}} \right)_t
$$

**Weight.** A smooth function of the regime, fitted by least squares against the mid 30 s later:

$$
w_t = \frac{1}{1 + e^{-(\beta_0 + \beta_1 f_{1,t} + \beta_2 f_{2,t})}}
$$

| Family | $h$ | $f_1$ | $f_2$ | $\beta_0, \beta_1, \beta_2$ (all days) | $w$ range |
|---|---|---|---|---|---|
| btc_15m | 120 s | minute / 14 | $\min(|d|, 3)/3$ | −2.53, −1.31, +1.28 | 0.02 to 0.22 |
| btc_daily_ladder | 2 s | $\ln(1 + \tau/3600) / \ln 49$ | $\min(|d|, 3)/3$ | +2.20, −2.24, −4.48 | 0.01 to 0.90 |
| fed_dec_2026 | – | – | – | microprices rescaled so $\sum_j p_j = 1$ | – |

Ladder prices are then forced to fall as the strike rises (pool-adjacent-violators per second).
Parameters for the next stages: `reports/fair/fair_params.json`, `reports/fair/stoikov_table.parquet`; the series per day: `reports/fair/<date>/fair.parquet`.

## 3. Checks that passed

| Check | Result |
|---|---|
| 15m `finalPrice` of window $k$ equals `priceToBeat` of window $k+1$ | 186 of 186 |
| Up exactly when final ≥ strike | 192 of 192 |
| Gamma outcome vs our recorded `market_resolved` | 123 of 123 agree |
| Ladder strikes parsed | 66 of 66 |
| Lookahead: every book, spot and $\sigma$ input received at or before $t$ | asserted on 4.2 M rows, 0 violations |
| Binance price age on live days, 99th percentile | 0.84 to 0.86 s |
| Binance–Chainlink gap at window start, 11-06 | 14.4 bp, spread 0.9 bp; drifts a median 0.45 bp within a window |
| Fed: sum of the five microprices | mean 0.9998, 1st to 99th percentile 0.990 to 1.012 |
| Tests | 77 pass, 17 of them for fair value (ruler on planted series, model limits, anchor, weight fit, lookahead guard) |

## 4. Held-out scores

Skill is $1 - \text{MSE}_{\text{candidate}} / \text{MSE}_{\text{mid}}$ against the mid $\tau$ seconds later, in log-odds, with standard errors clustered by 15-minute window (btc_15m) or market-hour (ladder).
Hit rate: among seconds where the candidate disagreed with the mid and the mid moved, the share where it moved toward the candidate.

**btc_15m** (121 windows, 107 k seconds)

| Candidate | Skill 5 s | Skill 30 s | Skill 60 s | Hit 5 s | Brier |
|---|---|---|---|---|---|
| mid | 0 | 0 | 0 | – | 0.1525 |
| microprice | −0.226 | −0.013 | 0.000 | 0.659 | 0.1525 |
| Stoikov (book) | **+0.020** | +0.009 | +0.006 | 0.654 | 0.1525 |
| BTC model, raw | −27.8 | −3.62 | −1.44 | 0.529 | 0.1611 |
| BTC model, anchored | −6.93 | −0.86 | −0.31 | 0.560 | 0.1522 |
| **fair** | −0.003 | **+0.016** ± 0.003 | **+0.014** ± 0.003 | 0.653 | **0.1521** |

By minute of the window, skill at 30 s:

| Minutes | Stoikov | fair |
|---|---|---|
| 0–4 | +0.013 | +0.012 |
| 5–9 | +0.014 | +0.017 |
| 10–12 | +0.013 | **+0.033** |
| 13–14 | +0.003 | +0.002 |

**btc_daily_ladder** (active books, mid 0.03 to 0.97; 210 market-hours, 680 k seconds)

| Candidate | Skill 5 s | Skill 30 s | Skill 60 s | Hit 5 s |
|---|---|---|---|---|
| Stoikov (book) | −0.017 | +0.004 | +0.005 | 0.550 |
| BTC model, anchored | −2.88 | −0.54 | −0.26 | **0.678** |
| **fair** | −0.037 | **+0.010** | **+0.010** | 0.631 |

**fed_dec_2026**: in the active Fed books the mid did not move once on the held-out days at 1 to 60 s, so skill is undefined. The only test is internal consistency: the outcomes sum to 1.

## 5. What perfecting each input would buy (oracles, btc_15m, anchored model)

Each oracle replaces one input by its true value, known only afterwards. They are upper bounds, not usable fair values.

| Input made perfect | Brier | $b$ at 30 s | Hit 5 s | Reading |
|---|---|---|---|---|
| none (anchored model) | 0.1522 | 0.082 | 0.560 | baseline |
| $\sigma$ = the window's realised vol | **0.1517** | **0.109** | **0.569** | the biggest gain: the volatility forecast matters most |
| Chainlink gap = true path | 0.1519 | 0.074 | 0.560 | helps calibration, little for direction |
| Binance with zero latency (128 ms earlier) | 0.1522 | 0.083 | 0.563 | worth nothing at these horizons |
| Binance one second ahead | 0.1526 | 0.092 | **0.577** | a 1 s lead helps the next few seconds |

So the ranking of future work is: the $\sigma$ forecast, then a faster or earlier BTC signal, then the Chainlink gap. Our 128 ms feed latency is not the bottleneck.

## 6. What this teaches (HFT quant track, skill 5: fair value)

- **Model levels and model changes are different products.** A pricing model can be wrong in level and still right about direction and timing. Desks routinely quote "last trade or mid, plus delta times the spot move since", which is the anchored model. The common mistake is to blend a model's level into a market price and conclude the model is useless.
- **In a 1-cent market, "skill vs the future mid" punishes level offsets brutally.** The mid moves rarely, so its own error is tiny and any candidate that sits off the mid inside the spread scores very negative at 1 s (the raw model −155) even when it predicts direction well (the microprice: 73 % at 1 s). Read the hit rate and the slope $b$ together with the skill; the skill is meaningful at 30 to 60 s.
- **The market absorbs BTC news slowly.** The share of the model–book gap the mid closes keeps rising out to 60 s and is largest in minutes 10 to 12 (about 0.30 at 60 s); on the ladder in the last hour it reaches 1 within 5 s. That slowness is exactly the stale-quote risk stage 4 will measure.
- **At the strike, the settlement source decides.** In the example window (`example_window_15m.png`) Binance sat on the strike for three minutes while the Polymarket mid fell from 0.6 to 0.03 and the window resolved Down: those traders were watching Chainlink, which was below. Our gap estimate has about 1 bp of noise, which is the whole distance from the strike there.
- **The mid has a favourite–longshot bias.** Where the mid says 0.25, only 12 % resolved Up; the raw model got that bin right (0.25 vs 22 %). Calibration is where a model can beat the market even when it loses on short-horizon prediction.

## 7. Limits

- Two held-out days, 121 fifteen-minute windows, about 3 ladder resolutions; the Fed has none. Every number here is provisional.
- 11-06 (training only) uses the laptop recording and Binance history trades shifted by the measured 128 ms latency; 11-07 before 11:39 UTC uses history trades too.
- The anchor half-life jumped between folds for btc_15m (2 s, then 120 s): the loss surface is flat there, so treat $h$ as weakly identified.
- $\sigma_t$ is the step-5 forecast unchanged (parked for your modelling phase); its hour profile is fitted leaving the forecast day out, but using the other days, which is mild lookahead.
- The weight is fitted at $\tau$ = 30 s, but $\hat{w}$ is still rising at 60 s, so markouts beyond a minute may want a weight fitted at their own horizon.

## 8. Version 2 candidates, ranked by the evidence above

1. **σ forecast** (your parked work): the largest oracle gain. Plugs into `sigma_s` without code changes.
2. **Record the Chainlink BTC/USD price.** Polymarket's real-time data feed has a free `crypto_prices_chainlink` topic (verify the current docs: they mention a migration to a newer stream). That removes the gap estimate entirely.
3. **A weight that grows with the gap in ticks.** The mid only moves when the fair value is more than half a tick away.
4. **An earlier BTC signal**: Binance futures or Coinbase, via a cross-venue lead–lag study; the +1 s oracle says it is worth having.

## 9. How to rerun

```bash
.venv/bin/polymm quality --date 2026-11-09 --data data_vps --out reports/fair/2026-11-09
.venv/bin/polymm vol --date 2026-11-09 --data data_vps --spot-data data_vps --replay-out reports/fair/2026-11-09/replay --strict reports/fair/2026-11-09/clean_windows_strict.parquet --out reports/fair/2026-11-09/vol --no-plots
.venv/bin/polymm fair 2026-11-06 2026-11-07 2026-11-08 2026-11-09
```
