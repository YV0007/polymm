# Strategy diagnosis: where the BTC 15m strategy loses money, and why

Built 2026-11-11 by `polymm diagnose` (code `src/polymm/sim/diagnose.py`, decision D-034), on top of `polymm sim` (D-032).
Tables: `reports/sim_diag/table_*.parquet`. Plots: `research/strategy_diagnosis/`. Everything below is out of sample (walk-forward settings) unless marked "exploratory".

## 0. The answer in one paragraph

**We lose because informed takers know where BTC 15m's fair value is going about half a second to a second before we do, and they trade against our quotes in that window.**

- **When the loss happens:** 79–87 % of the adverse move after our fills happens within the first second.
- **What would fix it:** a fair value known 1 s earlier would turn the main strategy from −4.54 to +3.45 USD per window.
- **What would not:** removing our own 127 ms Binance feed lag gains nothing, faster orders make it worse, and being first in the queue makes it worse.
- **Cancel speed** is a real but smaller lever, worth 18 % of the loss on rung 1 and enough to bring rung 3 to break-even.
- **Inventory is not the problem**, but the last minutes of each window are: near expiry fair value moves fastest and takers pre-empt most.
- **The market itself** pays the average resting maker about zero (+0.04 ¢ per share after the rebate). Our fills are 6–7× more adversely selected than that average, so the loss is ours (algo), inside a market that offers no free edge.
- **The gap between the prize and what we can reach is large.** What we already record predicts only 1–1.5 % of the next second's fair-value move. That is where the research has to go.

## 1. Context

| Item | Value |
|---|---|
| Market | Polymarket BTC 15-minute Up/Down, the live 15 min of each market, 96 windows a day |
| Days | 11-06 training only · 11-07, 11-08, 11-09 walk-forward test · 11-10 lockbox (opened once) |
| Windows analysed | 355 out of sample (259 test + 96 lockbox) |
| Replay | every book update (40–50 M a day), every trade, every Binance quote, on our receive clock |
| Strategies diagnosed | rung 1 and rung 3 of the simulator ladder, at the setting the walk-forward chose each day |
| Rung 1, "event-driven quoter" | bid at fair − h, ask at fair + h (h = 3 ¢); re-priced on every book or fair-value change; an order within 1 tick of its target is kept unless fair crossed it; inventory limit 100 shares |
| Rung 3, "+ jump guard" | rung 1, plus: when fair moves ≥ 0.5–1 ¢ in 0.5 s, pull the side the move will hit for 1–3 s; reduce-only in the last 60 s; h = 2–3 ¢ |
| Size | 10 shares a side |
| Latency | order path 75 ms (lognormal jitter, plus a burst term); Binance feed 127 ms late (in the data) |
| Fill model | stage 5's queue model, matched cancels (the conclusions hold under all four, D-032) |
| Fair value | stage 3's walk-forward value, moved by every Binance tick between seconds |
| Fees | taker fee 7 %·p(1−p); maker rebate 20 % of it, 1.4 %·p(1−p) per share; no reward pool |

## 2. How it performed

**General statistics** (all 355 out-of-sample windows, in time order; USD):

| Rung | P&L | Mean / window | SE | Sharpe / window | Sharpe, annualised | Windows won | Worst window | Max drawdown | P&L / day | Fills / day | Shares / day | Capital peak | Return on capital / day |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | −1,613 | −4.54 | 1.14 | −0.21 | −39.8 | 38 % | −73.2 | −1,624 | −403 | 2,642 | 19,335 | 145 | −277 % |
| 3 | −203 | −0.57 | 0.79 | −0.04 | −7.2 | 43 % | −76.0 | −344 | −51 | 1,037 | 7,906 | 129 | −39 % |

The annualised Sharpe is the per-window Sharpe × √(96 × 365). Capital peak = the most inventory held + both resting orders (an upper bound). Lockbox alone (11-10): rung 1 −5.69 ± 1.83, rung 3 −0.23 ± 0.97 USD per window, the same picture.

> **How a desk reads this:** a per-window Sharpe of −0.21 over 355 windows is a consistent loser, not noise (t ≈ −4). Rung 3 at −0.04 is indistinguishable from zero. Losing 0.4–2.8 × the capital a day at 10 shares means the per-share edge must change sign before size means anything.

**Where each filled share's P&L comes from** (¢ per filled share):

| | ① spread | ② by 1 s | ② 1–60 s | ③ inventory | ④ rebate | Net | Fills | Through-fills |
|---|---|---|---|---|---|---|---|---|
| Rung 1 | +3.67 | −5.04 | −1.32 | +0.31 | +0.29 | −2.09 | 10,567 | 66 % |
| Rung 3 | +2.86 | −3.91 | −0.60 | +0.73 | +0.28 | −0.64 | 4,149 | 64 % |
| Average resting maker (stage 4) | +0.70 | −0.72 | −0.19 | | +0.25 | +0.04 | 328 k | |

Through-fills are fills where the price traded through our level.

## 3. Where we lose

### 3.1 By how the fill happened

Each fill gets the first label that applies:

- **A, cancel in flight:** we had already decided to pull the order, and the taker beat our cancel.
- **B, stale, not pulled:** fair value had crossed our price and we had not reacted.
- **C, swept through:** a taker traded through our price while our fair value still said the quote was fine.
- **D, at our level:** an ordinary fill from the front of the queue.

| | Share of shares | ① ¢ | ② ¢ (by 1 s / 1–60 s) | Net ¢/share | USD | Share of the loss |
|---|---|---|---|---|---|---|
| **Rung 1** A cancel in flight | 21 % | +2.66 | −5.43 / −0.52 | −2.68 | −435 | 27 % |
| B stale, not pulled | 0.03 % | | | | +7 | 0 % |
| C swept through | 49 % | +4.00 | −5.75 / −2.22 | **−3.40** | **−1,297** | **80 %** |
| D at our level | 30 % | +3.85 | −3.57 / −0.39 | +0.48 | +111 | −7 % |
| **Rung 3** A cancel in flight | 22 % | +1.81 | −4.06 / −0.61 | −3.98 | −276 | 136 % |
| C swept through | 46 % | +3.19 | −4.59 / −1.56 | −2.29 | −335 | 165 % |
| D at our level | 32 % | +3.11 | −2.84 / +0.78 | **+3.96** | **+398** | −196 % |

- **B almost never happens:** both rungs re-price on every fair-value tick, so we are never sitting there knowingly stale.
- **C is the information problem:** when C fills happen our fair value says we are 4 ¢ in the money, and within a second it has moved 5.8 ¢ against us. The taker knew something our fair value did not yet contain.
- **A is the race:** we saw the move and sent the cancel, but it landed too late.
- **D, the "normal" market-making fills, make money**, a lot on rung 3. The strategy works when it trades with uninformed flow; it is overwhelmed by A + C.

### 3.2 When the loss arrives

Markout of our fills against fair value, ¢ per share, τ after the fill:

| τ | fill | 0.1 s | 0.25 s | 0.5 s | 1 s | 2 s | 5 s | 10 s | 60 s |
|---|---|---|---|---|---|---|---|---|---|
| Rung 1 | +3.67 | +2.98 | +2.02 | +0.56 | −1.37 | −2.02 | −2.54 | −2.69 | −2.69 |
| Rung 3 | +2.86 | +2.26 | +1.40 | +0.27 | −1.05 | −1.50 | −1.92 | −1.90 | −1.65 |
| Average maker | +0.70 | | | | −0.02 | −0.09 | −0.13 | −0.16 | −0.21 |

**79 % (rung 1) and 87 % (rung 3) of the drop from the fill to 60 s happens in the first second**, and two-thirds of that first-second drop comes between 0.25 and 1 s. A loss that arrives this fast is a race or a short-horizon prediction problem, not a fair-value-level or volatility problem: those would show a slow drift over tens of seconds (`signature.png`).

### 3.3 By state (rung 1, ¢ per filled share)

| Slice | Share of shares | ② by 1 s | Net | Share of the loss |
|---|---|---|---|---|
| Order filled < 100 ms after it reached the book | 21 % | −6.71 | **−3.67** | 38 % |
| 0.1–1 s old | 41 % | −4.98 | −1.84 | 36 % |
| 1–10 s old | 31 % | −4.08 | −1.22 | 18 % |
| Minute 0–4 of the window | 24 % | −4.22 | +0.95 | −11 % |
| Minute 5–9 | 28 % | −5.01 | −2.72 | 37 % |
| Minute 10–12 | 25 % | −4.93 | −2.23 | 27 % |
| **Minute 13–14** | 23 % | −6.02 | **−4.29** | **48 %** |
| Spread 1 ¢ at the fill | 53 % | −4.77 | −1.27 | 32 % |
| Spread 2–5 ¢ | 37 % | −5.0 | −3.0 | 54 % |
| Price-improving order | 14 % | −6.18 | **−4.84** | 34 % |
| At the touch | 18 % | −6.02 | −0.19 | 2 % |
| Fair moved ≥ 3 ¢ in the second before | 31 % | −4.78 | −2.63 | 40 % |
| Fair moved < 0.25 ¢ in the second before | 18 % | −5.17 | −3.01 | 25 % |

Three things stand out:

- **Fresh orders are the worst:** an order filled within 100 ms of landing was placed into a market already moving.
- **The last two minutes** carry half the loss on a quarter of the shares.
- **Calm moments are toxic too:** a quarter of the loss comes when our fair value had not moved at all in the previous second. Those are the C fills, information we don't have.

### 3.4 Three real fills (typical, not worst)

Rung 1, 10-share fills. Times are from the moment we sent the order; prices in cents.

| | A: cancel in flight | C: swept through | D: at our level |
|---|---|---|---|
| When | 11-07 18:34:32, minute 4 | 11-10 23:50:01, minute 5 | 11-09 22:25:27, minute 10 |
| Our order | ask at 74 (fair 70 → 4 ¢ above) | bid at 28 (fair 32 → 4 ¢ below) | bid at 39 (fair 43) |
| Reaches the book | +80 ms | +61 ms | +78 ms |
| Taker's order starts | +2,420 ms; fair 71 | +3,781 ms; fair 30, still 2 ¢ above our bid | +4,232 ms; fair 42 |
| What we were doing | fair had moved up, we had sent the cancel | nothing: our fair said the bid was fine | resting at the front of the queue |
| Markout +0.25 s | +2.9 ¢ | −1.4 ¢ (fair fell 3.4 ¢ within 250 ms) | +3.2 ¢ |
| Markout +1 s | −1.9 ¢ | −1.4 ¢ | +0.4 ¢ |
| Markout +60 s | −2.0 ¢ | −2.6 ¢ | −1.0 ¢ |

The C fill is the whole story in one row. The taker sold to us at 28 while our fair value said 30. A quarter of a second later the fair value said 26.7: the taker's information reached BTC 15m's fair value ~250 ms before it reached ours.

## 4. Why: the hypotheses, tested

**Oracles.** Each reruns the same setting with one handicap removed. The gain is the most that lever could ever be worth. Gains are not additive.

| Oracle (what we pretend to have) | Rung 1: mean | Rung 1: gain | Rung 3: mean | Rung 3: gain |
|---|---|---|---|---|
| Real setting (75 ms, our fair value) | −4.54 | | −0.57 | |
| Fair value 127 ms earlier (no Binance feed lag) | −4.77 | −0.23 ± 0.37 | −0.26 | +0.31 ± 0.26 |
| **Fair value 1 s earlier** | **+3.45** | **+8.00 ± 1.08** | **+1.67** | **+2.24 ± 0.65** |
| Fair value 5 s earlier | +15.62 | +20.16 | +9.35 | +9.92 |
| Fair value 60 s earlier (near-perfect) | +52.53 | +57.08 | +33.73 | +34.30 |
| Instant cancels | −3.73 | **+0.81 ± 0.39** | −0.02 | **+0.55 ± 0.27** |
| Zero order latency | −6.95 | −2.40 ± 0.65 | −0.41 | +0.16 ± 0.45 |
| First in the queue | −5.22 | −0.68 ± 0.55 | −0.72 | −0.15 ± 0.43 |
| Colocated (zero latency + no feed lag) | −5.80 | −1.26 ± 0.72 | −0.30 | +0.28 ± 0.57 |
| All: zero latency + fair 1 s earlier + first in queue | +10.15 | +14.70 | +5.64 | +6.21 |

Means and gains are USD per window, ± SE paired by window.

**The race.** 34,514 fair-value jumps of at least 1 ¢ within 0.5 s, as we receive them: 300–460 per live hour.

| Jump size | Jumps | Taker trades in the jump's direction in the 500 ms BEFORE we see it | Control: opposite direction | First taker AFTER we see it, median | Within our 75 ms |
|---|---|---|---|---|---|
| 1–2 ¢ | 20,854 | 29 % | 13 % | 1,095 ms | 8 % |
| 2–4 ¢ | 10,546 | 39 % | 15 % | 982 ms | 10 % |
| ≥ 4 ¢ | 3,114 | **60 %** | 23 % | 501 ms | 16 % |

**Predictability of the next second with what we already record.** Walk-forward ridge on the 1 s grid, over 255 k test seconds. Inputs: fair value's last moves, mid − fair gap, BTC 1 s / 5 s returns, Polymarket taker flow 1 s / 5 s, book imbalance.

| Test day | R², out of sample | Correlation | Sign right on moves ≥ 1 ¢ | Best single input |
|---|---|---|---|---|
| 11-08 | −0.11 (one training day) | 0.10 | 57.5 % | taker flow 1 s (0.19) |
| 11-09 | 0.011 | 0.12 | 55.9 % | BTC 1 s return (0.11) |
| 11-10 | 0.015 | 0.13 | 56.6 % | BTC 1 s return / taker flow (0.09–0.10) |

**Verdicts:**

| Hypothesis | Test | Result | Verdict |
|---|---|---|---|
| **H1 Volatility / fair value modelled poorly**, so quotes in the wrong place | oracle fair value 127 ms / 1 s / 60 s earlier; split of ② by horizon | Our fair value is not wrong on average. It is **late**, by a fraction of a second, relative to informed takers. Removing our own feed lag (127 ms) gains nothing; knowing 1 s ahead gains 8 USD per window. The slow part ② 1–60 s is 1.3 ¢ of 6.4 ¢ (≈ 20 %). | **Partly.** The fix is short-horizon prediction (0.25–1 s), not the volatility level. σ matters for the slow 20 % and for the steep last minutes. |
| **H2 Poor prediction of adverse selection**, so we join bad quoting rounds | walk-forward ridge on each order's send-time state | AUC for "this fill loses" 0.46–0.54. The model's kept orders make −0.24 instead of −3.44 USD per window (rung 1), but only by trading ~half as much, not by finding the toxic orders. | **Yes, but** the current inputs can't see toxicity. A selection model needs the same new signals as H1. |
| **H3 Latency / queue:** we can't get a good place, we pull, our stale quotes get picked off | zero order latency, instant cancels, first in queue, colocated | Faster orders: −2.40 (worse); first in queue: −0.68 (worse); colocated: −1.26 (worse); instant cancels: +0.81 (rung 1), +0.55 (rung 3, to break-even). | **Mostly no.** Raw speed and queue priority are not the bottleneck; being faster without better information only gets us filled faster by the same informed flow. The cancel race is a real secondary lever. |
| **H4 Inventory management** | ③ per share; the stop-adding-inventory rule (exploratory) | ③ is positive (+0.31 / +0.73 ¢). The last 2 minutes carry 48 % of rung 1's loss. Reduce-only in the last 3 min: rung 1 −4.54 → **−2.34**; rung 3 −0.57 → −0.47 (its jump guard already covers it). | **No** for inventory itself; **yes** for timing. Near expiry the fair value becomes very steep (a digital option), every BTC tick is a big move, and takers pre-empt most. |
| **H5 Faster / better-informed takers** | pre-emption vs control; arrival after our jump; the 1 s oracle | 29–60 % of jumps have a same-direction taker trade before our feed shows the jump, against 13–23 % by chance, rising with jump size. | **Yes, the main cause.** They are early by more than our feed lag, so it is prediction or a faster source of BTC (perpetual futures, other venues, Chainlink), not only geography. |

## 5. Market vs algo

The split: **our P&L per share = what the market pays the average resting maker + our gap to that maker.**

| Per filled share, at 60 s | Market baseline (average maker) | Ours, rung 1 | Gap (algo) |
|---|---|---|---|
| ① spread captured | +0.70 | +3.67 | +2.97 |
| Markout to 60 s | −0.91 | −6.36 | −5.45 |
| Rebate | +0.25 | +0.29 | +0.04 |
| **Net** | **+0.04** | **−2.40** | **−2.44** |

- **The market** gives a passive maker about zero after the rebate. Whoever these makers are (mostly fast incumbents at the touch with queue priority), they break even. BTC 15m has no free edge for a typical maker, and no reward pool to add one.
- **The algo** loses 2.4 ¢ per share relative to that average. We quote wider and capture 3 ¢ more spread, but we are filled about 7× more selectively when the price is about to run (markout to 60 s −6.36 ¢ vs −0.91 ¢).

**Market factors**, the numbers to put next to any other market (BTC 15m, per day):

| Factor | 11-07 | 11-08 | 11-09 | 11-10 | What it means for a maker |
|---|---|---|---|---|---|
| Time at a 1-tick spread | 98 % | 98 % | 98 % | 98 % | no room to quote inside; queue and timing decide |
| Median size at the touch (shares) | 170 | 196 | 374 | 420 | deep queues; our place is at the back |
| Fair-value move, SD per second | 1.46 ¢ | 1.29 ¢ | 1.37 ¢ | 1.44 ¢ | > 1 tick per second: a 1-tick quote is stale within a second |
| Jumps ≥ 1 ¢ in 0.5 s, per live hour | 307 | 321 | 461 | 461 | about one every 8–12 s |
| Takers ahead of our feed (vs control) | 43 vs 18 % | 47 vs 23 % | 28 vs 11 % | 30 vs 11 % | how informed and fast the takers are |
| Average maker net per share (60 s, incl. rebate) | −0.72 | +0.17 | +0.06 | +0.20 | the market's baseline for a maker |
| Rewards | none | none | none | none | no subsidy for resting |
| Driver | BTC, priced on another venue first | | | | an external, faster price exists |

**What is market and what is algo:**

| Fixed by the market (choose markets on it) | In our power (optimise the algo) |
|---|---|
| 1-tick floor and deep touch queues | where and when we quote (minutes, after jumps, fresh vs resting) |
| a faster external driver (BTC on Binance and others) and the informed takers it attracts | how early we know the fair value (short-horizon prediction, faster sources) |
| fees and rebate, no reward pool | how fast our cancels land |
| the 15-minute binary payoff, steep near expiry | how much inventory we carry into expiry |
| the average maker's break-even | whether we are better than the average maker: so far, worse |

The line is not absolute. A market's rules (the 1 s taker delay on sports, settlement on a 60 s average) can be modelled into an edge. But for comparing markets, the top three market factors are the ones to measure first: **baseline maker net**, **takers ahead of the external driver**, and **jump rate relative to the tick**. BTC 15m scores badly on all three: zero baseline, strong pre-emption, a jump every 10 s on a 1-tick book.

## 6. Levers: what could change the sign, and in what order

| # | Lever | Fixes | Evidence | Most it can be worth (USD/window) | Realistic now? | Effort | Priority |
|---|---|---|---|---|---|---|---|
| 1 | **Short-horizon prediction of fair value (0.25–1 s)**: new inputs: Binance perpetual futures order flow and book, other BTC venues, Chainlink stream, Polymarket order flow and book dynamics | C (80 %) and A | 1 s oracle +8.0 (rung 1), +2.2 (rung 3); 79–87 % of ② lands within 1 s; takers pre-empt 29–60 % of jumps | +8 (rung 1), +2.2 (rung 3) | unknown: today's inputs reach R² ≈ 1 % | medium–high: record new feeds, model, test | **1, research** |
| 2 | **Exposure timing:** reduce-only in the last 2–3 min; pull around jumps | late-window losses, A | minute 13–14 = 48 % of loss; tested: −4.54 → −2.34 (rung 1); jump guard −5.30 → −0.52 (D-032) | +2.2 (rung 1); rung 3 already has most of it | yes | low | **1, now** |
| 3 | **Don't chase:** no price-improving or fresh orders into a moving fair value; longer minimum resting | fresh-order fills | fills < 100 ms after landing −3.67 ¢; price-improving −4.84 ¢ | up to ~1.7 (rung 1; those buckets' share of the loss), path effects unknown | yes | low | **2** |
| 4 | **Faster cancels:** measure real cancel latency, server region, warm connections, cancel-all on a signal | A | instant-cancel oracle +0.81 / +0.55 (rung 3 to break-even) | +0.8 / +0.55 | partly: maybe a third to a half of it | low–medium (infra, D-031) | **2** |
| 5 | **Selection model (ML on order state)** | C, A | AUC 0.46–0.54 with today's inputs; gains so far only from trading less | unknown until #1's inputs exist | no, not yet | medium | 3, after #1 |
| 6 | **Volatility and fair-value level** (σ forecast, Chainlink gap, end-of-window settlement average) | ② 1–60 s (≈ 20 % of ②), steepness near expiry | ② 1–60 s = −1.3 ¢ (rung 1); stage 3 ranked the σ forecast first for 30–60 s fair skill (D-024) | ≈ 20 % of the ② loss | yes | medium | 3 |
| 7 | Raw order speed, queue priority | none | oracles negative or zero | ≤ 0 | | | don't, on their own |
| 8 | **Market screening on the market factors**: baseline maker net, pre-emption vs the external driver, jump rate per tick, rewards per capital | the market side | BTC 15m: zero baseline, strong pre-emption, no rewards | n/a | yes: record candidate books, compute §5's table | low | **1, in parallel** |

**The ordering reasoning:**

1. **#2 and #3 are cheap and should go into the ladder now.** They don't make BTC 15m profitable on their own: rung 1 + #2 is still −2.34 per window.
2. **#1 is the only lever big enough to change the sign**, and its prize is real (+8 USD per window at 1 s). But nothing we record today reaches it: R² 1 %. So it starts as research. Record the faster BTC sources, measure how much of the next second they predict, then plug the prediction into the simulator and measure how much of the oracle gain survives.
3. **#4 adds a little and is infrastructure we need anyway.**
4. **#5 and #6 come after #1**, because they need its inputs or address the smaller slice.
5. **#8 runs in parallel.** Recording books is cheap, and §5's table says what to look for: markets whose average maker breaks even or better, whose takers are not ahead of a faster external price, and that pay rewards. BTC 15m is the hardest kind: an external price, priced faster elsewhere, on a 1-tick book.

## 7. Limitations

- Four out-of-sample days, one market, 10-share quotes, no market impact (our orders never change others' behaviour).
- **Oracles are upper bounds.** "Fair value 1 s earlier" also knows where the book will be in 1 s; a real predictor captures a fraction. Gains are not additive.
- **The selection counterfactual drops orders after the fact** and ignores how the path would change: an estimate.
- **The end-of-window rule is exploratory:** I chose it after seeing minute 13–14 in these same days.
- **The average maker** (stage 4) is everyone's public fills at the touch, mixing fast incumbents with slow ones. It is a baseline, not a competitor we can see individually (no L3).
- **Pre-emption is measured against our receive time.** It mixes geography (Binance is in Tokyo) with prediction, and the 127 ms oracle says geography alone is not it.

> **Note (skills 3 and 5: adverse selection, cross-venue price discovery):** the markout curve is the first plot a desk asks for. A loss that lands within a second means you are trading against someone with a faster or better signal, a pricing race. A slow drift over minutes means your fair value is wrong, a modelling problem. Here it's the race, so the fix is upstream (information) or avoidance (timing and selection), not a better volatility model or more speed.
>
> **Note (skill 8: presenting findings):** the oracle table is the standard way to rank levers before building any of them. Each row is "if this part were perfect, what would it be worth". The common mistake is building the lever that's easiest to build, here faster orders, which the oracle shows would lose more.
