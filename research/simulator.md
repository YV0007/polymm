# Simulator v1 (stage 7)

Built 2026-11-10 by `polymm sim 2026-11-06 2026-11-07 2026-11-08 2026-11-09` (code `src/polymm/sim/`, decision D-032).
11-06 is training only, 11-07 and 11-08 are the walk-forward test days, 11-09 is the lockbox (never used to choose anything).
Machine-written tables: `reports/sim/summary.md`. Plots: `research/simulator/`. Settings and selections: `reports/sim/sim_params.json`.

## 1. Result in one paragraph

> **Status: v1 floor, not a verdict (D-027).** The engine is validated (it reproduces stage 5's fills exactly, and every check below passes). The strategies are the first, simple rungs, quoting 10 shares on BTC 15m with stage 3's fair value and a 75 ms order path.

**Gate B fails on every rung.** No strategy makes money out of sample on BTC 15m. On every rung the fills lose more to adverse selection than the quotes capture in spread: ② exceeds ① by 1.6–2.5 ¢ per filled share at 60 s, and the rebate (0.29 ¢) closes only a fraction of that.

- **The jump guard is the one rule that moves the result.** Rung 3 pulls the side a fair-value jump will hit. It halves the fills and cuts the loss per window from −5.30 to −0.52 USD (± 1.54, test days), but does not make it positive. On the lockbox it is −1.18 ± 0.82.
- **The selective rung declines to trade.** Rung 4 uses stage 5's value table, in which almost every cell is negative. It sat out both test days and lost −0.44 ± 0.48 on the lockbox.
- **Inventory skew** (rung 2) changes little, since inventory is not where the loss is.
- **Fill model doesn't matter.** Every conclusion is the same under all four cancel models and on gap-free windows only.
- **The Fed rewards rung** earns 0.24–0.51 USD a day in rewards against fill P&L of −14 to +1.4 USD a day.

## 2. Results

**The ladder, walk-forward** (test days 11-07 and 11-08, 163 windows; matched cancels, 75 ms; USD per 15-minute window, 10-share quotes):

| Rung | What it adds | Mean | SE | Fills | ① | ② | ③ | ④ | Net |
|---|---|---|---|---|---|---|---|---|---|
| 0 | symmetric, 2 s timer | −3.41 | 1.65 | 5,041 | +2.38 | −4.62 | +0.40 | +0.29 | −1.55 |
| 1 | event-driven (stale cancel) | −5.30 | 2.03 | 6,814 | +3.78 | −6.26 | +0.48 | +0.29 | −1.71 |
| 2 | + reservation price | −5.04 | 1.61 | 7,525 | +3.64 | −5.60 | +0.20 | +0.29 | −1.46 |
| 3 | + jump guard | −0.52 | 1.54 | 3,144 | +2.90 | −4.48 | +0.93 | +0.29 | −0.35 |
| 4 | + selective (stage-5 table) | 0 | 0 | 0 | | | | | |

① to ④ and Net are in cents per filled share. ① = spread captured against fair value at the taker order's start; ② = fair-value move against us by 60 s; ③ = holding to settlement; ④ = rebate.

**Lockbox (11-09, 96 windows):**

| Rung | 0 | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|
| Mean (USD/window) | −2.27 ± 1.42 | −1.85 ± 1.59 | −1.20 ± 1.08 | −1.18 ± 0.82 | −0.44 ± 0.48 |

**Fill-model band** (test days, USD per window, pessimistic / matched / optimistic):

| Rung | Pessimistic | Matched | Optimistic |
|---|---|---|---|
| 0 | −3.37 | −3.41 | −4.16 |
| 1 | −5.49 | −5.30 | −6.03 |
| 2 | −4.91 | −5.04 | −5.16 |
| 3 | −0.26 | −0.52 | −0.20 |

The band is narrow because most of our 10-share fills come from prices trading **through** our level, which the cancel model does not touch. More fills is not better here: the optimistic model fills more and loses more.

**Gate B, per rung (all FAIL):**

| Rung | Pessimistic lower 95 % bound | Lockbox mean > 0 | Deflated Sharpe | Plateau |
|---|---|---|---|---|
| 0 | −6.53 | no | 0.00 | no |
| 1 | −9.45 | no | 0.00 | yes |
| 2 | −8.08 | no | 0.00 | yes |
| 3 | −3.22 | no | 0.00 | yes |
| 4 | 0 (no trades) | no | — | yes |

The bound is per window, in USD. The deflated Sharpe uses 78 trials, with $SR_0$ = 0.42 per window. The plateau test is the share of grid neighbours within 1 SE of the best setting, ≥ 0.5 counting as a plateau.

**Windows needed** to tell the rung-3 mean from zero at its spread: $n \ge (2\sigma/\mu)^2 \approx$ 5,700 windows, about 59 days. If its true mean is near −0.5 USD per window, more data would show a loss, not an edge.

**Latency** (test days, USD per window, matched):

| Rung | 0 ms | 30 | 75 | 128 | 300 | 1000 |
|---|---|---|---|---|---|---|
| 0 | −4.56 | −3.98 | −3.41 | −3.63 | −4.97 | −5.45 |
| 1 | −8.42 | −7.68 | −5.30 | −4.94 | −5.17 | −5.46 |
| 3 | −0.23 | −0.66 | −0.52 | −0.27 | −1.72 | −3.76 |

Rungs 1 and 2 do *worse* at 0 ms than at 75–128 ms, though within 2 SE, so the delay check passes. The mechanism, measured on 11-08:

- The event-driven quoter re-prices on every fair-value tick. At 0 ms its price-improving orders land instantly and get filled: 1,683 fills against 654 at 75 ms, 83 % of them by the price running through us, at about −7 ¢ adverse selection each.
- At 75 ms, 1,695 of those orders are **rejected on arrival** (post-only) because the book has already moved through them.

Latency acts as an accidental filter on the orders that chase a move. Speed only pays when the signal is good, as rung 3's flat curve up to 128 ms suggests.

**Fed December, rung 5** (the only recorded markets with a reward pool; descriptive):

| Day | Rewards (USD) | Fill P&L, marked (USD) | Fills | Our pool share |
|---|---|---|---|---|
| 11-07 | 0.24 | +1.41 | 1 | 0.03 % |
| 11-08 | 0.28 | −0.02 | 13 | 0.04 % |
| 11-09 | 0.51 | −14.07 | 44 | 0.07 % |

Fill P&L is ①+②+③ plus the rebate, marked to the day's last fair value; h = 1 ¢; 200 shares a side. Our share of the 500 + 200 USD/day pools is under 0.1 %, because the Fed books are deep. 3 of the 5 Fed tokens trade near 0.006 and are outside the 0.05–0.95 quoting band.

## 3. What it means

1. **On BTC 15m, a quote resting near fair value is adverse-selected by more than its spread, at every distance we tried.** Rung 1 at h = 3 ¢ captures 3.8 ¢ per share and gives back 6.3 ¢ by 60 s. This is stage 5's finding (every placement loses) holding for a full strategy with inventory, re-quoting and a tick-level fair value.
2. **The money is in not being there.** Pulling the side a BTC jump will hit removes about 90 % of the loss. The stage-3 oracle ranking (D-024) said the same thing from the other direction: a σ forecast and an earlier BTC signal are worth more than anything else.
3. **The surfaces rise toward the grid's edge**, with wider h and a more sensitive jump trigger (see `research/simulator/stability.png`). The best settings may lie outside this grid. That is the first thing to extend, at the cost of more trials in the deflated Sharpe.
4. **Rewards are not the answer on the markets we record.** On the Fed, our 200-share quotes earn half a dollar a day at most, against inventory swings up to about 30 times larger.

**Levers, ranked by the attribution:** ② dominates every rung.

1. A better jump signal: event-time Binance moves, a σ forecast, an earlier BTC venue (D-031).
2. Wider and asymmetric quotes when a jump is likely.
3. Quoting only the side the signal favours.

Inventory (③) and rewards (④) are second-order on BTC 15m.

## 4. How it works

**Engine** (`sim/engine.py`, numba). For one market segment (a strict clean window inside the live window), it runs every event in receive-clock order:

- top of book;
- a level update, which moves our queue position by stage 5's model: consumption from the front, cancels by one of four cancel models, $x \le L$;
- a trade through our price, which fills us;
- a fair-value tick;
- our own events: an order arriving, a cancel taking effect, the strategy timer.

Market events at time t come before our own events at t, which is stage 5's convention. Rules on our orders:

- an order is rejected on arrival if it would cross (post-only);
- a cancel never lands before its order;
- a fill before a cancel lands counts.

**Latency (D-031).** Only the order path is added, since the replay clock already contains our feed lag:

$$
L = L_{\text{base}} \, e^{0.126 Z} + \text{burst}
$$

with $L_{\text{base}}$ = 75 ms. The jitter matches the measured VPS round trip. The burst term is the Polymarket feed lag above its normal 12 ms in the previous second.

**Fair value between seconds.** Stage 3's walk-forward fair value at each second, moved by every Binance mid change:

$$
x(t) = x_{\text{fair}}(k) + w(k)\,\big[x_{\text{model}}(S_t) - x_{\text{model}}(S_k)\big]
$$

Every other model input is held at second $k$. Three days have about 120 k ticks between seconds.

**Strategy ladder.** Every rung keeps the inventory limit $\lvert q \rvert \le 100$.

| Rung | Rule |
|---|---|
| 0 | quote $m \pm h$, re-quote every 2 s |
| 1 | re-quote on every book or fair change; keep an order within `hyst` ticks unless fair crossed it |
| 2 | reservation price $r = m - q\gamma\sigma_p^2(T-t)$, lean capped at 5 ¢; last `end_s` seconds only the reducing side |
| 3 | pull the side a jump will hit: fair moved ≥ `jump_c` in 0.5 s, pull for `hold_ms` |
| 4 | choose improve / touch / behind by stage 5's value table V̂ (earlier days only); quote only if V̂ > `v_thr`, never beyond $r$ |
| 5 | (reward market) join the best price if it is on our side of $r$ and within $h$, else rest at $r \mp h$; rewards scored after the run with stage 6's rules |

**Attribution** (`sim/attrib.py`). Per fill, in USD: ① $s(m-P)q$, ② $s(m^{\tau}-m)q$, ③ $s(V-m^{\tau})q$, ④ $r f P(1-P) q$. Here $m$ is the fair value at the taker order's start, $\tau$ = 60 s, and $V$ is the outcome (or the day's last fair value when unresolved). ①+②+③ = $s(V-P)q$ exactly, because we never exit as a taker.

**Experiments** (`sim/experiments.py`, `sim/run.py`):

- **Pass 1:** all 78 settings on every day (matched cancels, 75 ms). Selection for test day $k$ uses only days $< k$: the best mean per window. The lockbox is the last day.
- **Pass 2:** each selected setting under the four cancel models and six latencies, plus the checks.
- **Unit of independence:** one 15-minute window.
- **Trial log:** every setting × day in `reports/sim/<date>/grid_windows.parquet`.

## 5. Checks (all pass)

| Check | Result |
|---|---|
| Stage-5 probes replayed as scripted orders (every 7th grid probe, 3 cancel models) | fill quantity identical 100 %, rejections 100 % (19,398 probes on 11-08 during the build, 13,461 on 11-07 in the run) |
| First-fill time vs stage 5 | 99.1–99.8 % identical; 100 % with the 1e-12-share fill tolerance removed (stage 5 counts float-noise volume as a first fill) |
| ①+②+③ = hold-to-settlement P&L | max error 1.8e-15 USD |
| Fair value at whole seconds = stage 3/4's walk-forward series | exact (0.0) |
| Tick formula at the second's own BTC price = that second's fair value | max error 4e-12 |
| Do-nothing strategy | 0 fills on every day |
| A noise "fair value" (0.5 and 2 ¢ noise) never beats the true one | 4 of 4 |
| More latency never pays beyond 2 SE | 0 violations |
| Fills or order-log rows truncated | 0 |
| Unit tests (`tests/test_sim.py`, toy books with worked answers) | 11 pass |

## 6. Audit (independent review before commit)

| # | Finding | Fix |
|---|---|---|
| 1 | γ grid ~1000× too large (γ = 5 leaned quotes by dollars); found the same day on the first run | γ ∈ {0.001, 0.003, 0.01} (0.3 / 1 / 3 ¢ lean at 100 shares at the median $\sigma_p^2(T-t)$ = 0.033); lean capped at 5 ¢; rerun |
| 2 | Rung 5 joined a best bid *above* fair | join only if $0 \le r - \text{bid} \le h$; test added |
| 3 | Rewards averaged over covered minutes only (11-07: 882 of 1440, 1.6× overstated) | divide by the day's minutes; drop only the minutes where the rebuilt book failed the top-of-book check |
| 4 | Orders vanish at a data gap; live they would rest blind | gap-free-windows table (same picture: rung 3 −0.87, rung 1 −5.40 per window) |
| 5 | Sanity checks did not veto Gate B | they do now |
| 6 | Fair-grid check compared a copy with itself | now against `fair_oos`, plus the tick-formula self-check |
| 7 | Rewards used the order's final fill for its whole life; order-log truncation silent | fills by time; truncation counted and checked (0) |
| 8 | Day 0 in-sample in the trial Sharpes | excluded from the deflated-Sharpe trial spread |

Also found while building:

- Event-driven rungs waited for the first market event before quoting, so a one-shot decision was added at segment start.
- The rate limiter's first timestamp overflowed int64, which sent no orders at all; caught by the first smoke test.

## 7. Limitations (what v1 does not know)

- **Others don't react to us.** Our 10-share orders never change the book (the small-order assumption). Live, our quotes may attract the very flow that hurts most. Only the sim-vs-live comparison of stage 8 can measure this.
- **Venue processing time is assumed** (50 ms), not measured. The latency curve shows the conclusion holds from 0 to 128 ms.
- **Four days, one regime**: 259 BTC 15m windows outside training. The confidence intervals are wide. Rung 3's mean is not distinguishable from zero, and would need about 59 days to be.
- **The strategies are first rungs.** The fair value is stage 3's v1, the jump trigger is a fixed threshold, the value table is from blind stage-5 probes, and there is no σ forecast. Each is a lever, not a verdict (D-027).
- **Inventory is known about 18 ms early** (q updates at the taker order's start, not when the fill message would arrive). This is small and slightly optimistic.
- **The Fed is unresolved in the sample**, so its inventory is marked to fair value. Three of its five tokens trade near 0.006, outside the quoting band.
