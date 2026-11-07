# Shape of the market: stylized facts per family (stage 2)

Date: 2026-11-07. Code: `src/polymm/shape.py`, CLI `polymm shape`, tests `tests/test_shape.py`.
Reports: `reports/2026-11-06/shape/shape.md` (laptop block, 2026-11-06 13:10 → 11-07 00:25 UTC, 11 h 15 min) and
`reports/2026-11-07/shape/shape.md` (VPS, 09:35 → 11:55 UTC, 2 h 20 min, Binance from 11:39). Each report has the
full tables, one PNG per section and the frames as CSV under `tables/`.

**Everything here is provisional: one weekday afternoon/evening (laptop) and one Saturday morning (VPS).** Numbers
are quoted as "laptop | VPS" where both exist; `n` is clean seconds (time-weighted statistics) or events.

## 0. Method in five lines

- Only strict clean windows (`clean_windows_strict.parquet`, replay verified against REST), outcome-0 (Yes/Up) books.
- Book state is sampled on the replay's 1-second as-of grid (`depth_1s`): every second weighs the same, so shares are
  time shares and quantiles are time-weighted. A row-per-event average would over-weight the busy minutes 8× here.
- Ticks are cents (all three families are 1-cent markets; the 0.001 regime beyond 0.96/0.04 shows up as "<1 tick").
- Flow = `events_labeled` adds/cancels on the outcome-0 token per market-minute; fills = `trades` (one row per maker
  hit), grouped into taker orders by a 50 ms gap on the same token and side (hashes are per message in this feed).
  The replay's `label = 'fill'` is not used anywhere.
- Jumps = |mid(t) − mid(t−1 s)| on the grid, and aligned 10-s blocks; counted per hour of clean time.

## 1. btc_15m: BTC Up/Down 15-minute windows

| statistic | laptop (45 windows, 111 k valid s) | VPS (9 windows, 22 k valid s) |
|---|---|---|
| time at a 1-tick spread (live window, minutes 0-11) | 97-99 % | 99-100 % |
| time at 1 tick in minutes 12-14 | 84 / 78 / 80 % (sub-tick regime grows to 15 %) | 85 / 73 / 85 % |
| one side empty, minute 12 / 13 / 14 | 20 / 38 / 83 % of seconds | 11 / 15 / 72 % |
| $ at best bid / best ask, p50 (p25-p75) | $62 ($18-207) / $42 ($16-144) | $23 / $28 |
| $ within 3 ticks of mid per side, p50 | bid $462, ask $418 | $204 / $236 |
| depth within 3 ticks (both sides) by minute: 0 → 9 → 13 → 14 | $1,774 → $1,273 → $161 → $0 | $1,366 → $715 → $145 → $0 |
| taker order size p50 / p90 / p99 | $7 / $64 / $341 (35,549 orders) | $7 / $55 / $289 (6,283) |
| median trade message vs the level it hit | takes 15 % of the level; 26 % of messages wipe the level | 24 %; 28 % |
| taker orders per market-minute, minute 0-8 / 9-12 / 13 | 30-50 / 56-68 / 100 | 18-31 / 60-96 / 108 |
| taker direction (Yes frame), count / volume | 49.8 % / 48.4 % buys | 52.8 / 51.6 % |
| multi-maker orders; sweeps (≥ 2 levels); volume in sweeps | 20 %; 7.5 %; 24.5 % (sweep p90 6 ticks, max 77) | 20.5 %; 9.6 %; 31.8 % (max 38) |
| adds + cancels per market-minute, live window | ~10,000 + ~9,000 (3,460 + 3,100 incl. pre-window) | ~5,300 + ~5,000 |
| added size cancelled at its level within 1 / 10 / 60 s (upper bound) | 80 / 95 / 97 % | 65 / 92 / 95 % |
| 1-s mid moves ≥ 2 / ≥ 5 / ≥ 10 ticks per hour (whole family) | 200 / 30 / 4 | 117 / 25 / 8 |
| … in minute 0 / 9 / 13 / 14 | 593 / 617 / 1,153 / 1,899 (≥ 5 t: 47 / 104 / 376 / 872) | 213 / 417 / 819 / 528 |
| 10-s moves ≥ 5 ticks per hour, minute 0-8 / 13 / 14 | 68-110 / 155 / 275 | 42-96 / 118 / 139 |
| jump size p50 / p90 / max (1-s moves ≥ 2 ticks) | 2 / 5 / 50 ticks | 3 / 7 / 32 |
| 1-s jumps preceded by a ≥ 0.01 % Binance move in the previous 10 s | – (no spot) | 18.6 % vs 2.2 % base rate (70 jumps, 1,890 s) |
| pre-window (subscribed up to ~30 min early): spread at 1 tick, |mid − 0.5|, orders/min | 88 %, 0.005, 0.65 | 97 %, 0.005, 0.68 |

What it means for a market maker:

- **The tick floor binds all the time.** 97-99 % of live seconds sit at a 1-cent spread; p90 is 1 tick in every
  minute. Nobody can earn more than half a cent of half-spread here, so the competition is for queue position and
  the only knobs are *when* to be in the queue and on *which* side.
- **The book is thin against the flow at the top, deep two levels down.** The best level holds ~$40-60 (p50) while
  a median taker order is $7 and the p90 order is $64: one order in four wipes the level it hits, and 8-10 % of
  orders walk two or more levels. Within 3 ticks there is ~$450 per side, so a $100-200 quote does not move the
  book but is a large share of the touch.
- **The book is a flicker.** 10,000 adds and 9,000 cancels per minute per market during the live window, and 80 %
  of added size is gone from its level within 1 s (upper bound). Quotes are being re-priced continuously by bots;
  a resting order's queue position is reset constantly. Any queue model must be fitted on VPS data (laptop stalls).
- **It is jumpy, and it gets jumpier.** 2-tick moves within a second happen 600 times an hour in minute 0 and
  1,900 times an hour in minute 14; 5-tick moves go from 47/h to 870/h. Depth within 3 ticks falls from $1,800 to
  $160 by minute 13 and one side is empty 38 % of the time; by minute 14, 83 %. **Minutes 12-14 are not quotable**
  with a symmetric quoter: the book is one-sided and every move is a sweep of stale levels.
- **Direction is balanced** (49.8 % buys by count, 48.4 % by volume), so inventory does not drift on average, but it
  arrives in sweeps (24.5 % of volume).
- Verdict: **candidate for queue-position market making in minutes 0-9 only**, with cancellation on BTC moves
  (Binance leads by ~1 s, quality report; 18.6 % of jumps are preceded by a ≥ 0.01 % Binance move vs 2.2 % base
  rate, n = 70). Edge, if any, must come from term ② being small for *early* fills; stage 3 (markouts by minute)
  decides. Rewards (50 shares / 4.5 ¢) are irrelevant to the economics at this churn.

## 2. btc_daily_ladder: "Bitcoin above $X on <date>" (12:00 ET resolution)

| statistic | laptop (29 strikes × dates, 781 k valid s) | VPS (25, 168 k valid s) |
|---|---|---|
| time at a 1-tick spread, all strikes | 12.8 % (80 % sub-tick: far strikes at 0.1-0.5 ¢) | 11.3 % |
| strikes within 0.5 % of BTC: time at 1 tick / p50 / p90 spread | 43 % / 1.6 / 3.3 ticks (63 k s, 3 tokens) | 52 % / 1.1 / 3.5 (18 k s) |
| strikes 0.5-1 % away | 29 % / 2.1 / 4.0 (22 k s) | 48 % / 1.9 / 3.3 (5 k s) |
| strikes > 5 % away: sub-tick time; one side empty | 97 %; 28 % | 100 %; 48 % |
| $ at best bid / ask, p50 (p25-p75), all strikes | $9 ($2-84) / $7 ($2-67) | $5 / $8 |
| $ within 3 ticks per side p50, all strikes | bid $220, ask $227 (p25 $11 / $26) | $139 / $400 |
| … strikes within 0.5 % of BTC (both sides, p50, p25-p75) | $834 ($357-2,220) | $2,898 ($971-7,721) |
| depth imbalance within 3 ticks: share of seconds with \|imb\| > 0.5 | 68 % | 54 % |
| taker order size p50 / p90 / p99 | $10 / $299 / $2,872 (2,151 orders) | $5.5 / $112 / $1,730 (155) |
| median trade vs the level it hit; levels wiped | 66 %; 42 % | 95 %; 45 % |
| taker orders per market-minute: > 48 h / 24-48 h / 12-24 h / 1-3 h / < 1 h to expiry | 0.024 / 0.053 / 0.087 / 0.61 / 0.56 | 0.008 / 0.011 / – / – / – |
| adds + cancels per market-minute | 227 + 201 (h<1: 317 + 314) | 80 + 69 |
| added size cancelled within 1 / 10 / 60 s (upper bound) | 53 / 80 / 92 % | 27 / 64 / 76 % |
| 1-s mid moves ≥ 2 ticks per hour: all / 1-3 h / < 1 h to expiry | 2.1 / 5.9 / 97.6 (≥ 5 t: 0.46 / 0 / 29) | 0.21 |
| taker direction (Yes frame), count / volume | 48 % / 44 % buys | 42 % / 73 % (161 msgs) |

What it means:

- **The ladder is two markets.** Strikes more than 2 % from spot (two thirds of the token-time) are 0.1-0.5-cent
  books with one side empty a quarter of the time and no flow (0.02-0.05 orders per minute): nothing to make a
  market in. Strikes within 1 % of spot are 1-4-tick books with $800-3,000 within 3 ticks and quotes that are wider
  than the tick 57-70 % of the time: **the tick floor does not bind there**, so price competition is possible and a
  wider quote is a choice, not a constraint.
- **Flow is rare and lumpy.** 0.13 orders per market-minute overall; a median order is $10 but p90 is $300 and
  p99 $2,900, and a median trade takes two thirds of the level it hits (42 % wipe it). With a book this thin and
  this imbalanced (|imbalance| > 0.5 two thirds of the time) a $200 quote *is* the book on one side.
- **The last three hours are a different regime:** orders go from 0.05 to 0.6 per minute, 2-tick jumps from
  0.2/h to 6/h (1-3 h) and 98/h in the final hour, 5-tick jumps 29/h in the final hour. The ladder resolves on a
  single Binance 1-minute close, so the last hour is a digital option with σ√T → 0: quoting it is directional
  betting on the close, not market making.
- Verdict: **quotable at the near-the-money strikes 3-48 h before expiry**, with a quote 2-3 ticks wide and
  small size, as a *fair-value-model* strategy (the Φ(d₂) price from Binance) rather than a queue strategy: at
  0.05-0.1 orders per minute the earning rate is tiny unless the quote is wide and right. Stage 4 (fair value)
  matters more than queue modelling here.

## 3. fed_dec_2026: Fed December 2026 decision (neg-risk, rewarded)

The event has five markets; two are live (hike 25 bp at ~0.77, no change at ~0.22) and three are dead (< 1 ¢).
Pooled numbers mix them, so the per-market table in each report (section 8) is the one to read.

| statistic | laptop (193 k valid s, 5 markets) | VPS (37 k valid s) |
|---|---|---|
| live markets: time at a 1-tick spread | 100 % (both) | 100 % |
| dead markets: spread | 0.1 ¢ (0.001 regime), 100 % of the time | same |
| $ at best bid / best ask, p50, all five pooled | $1,255 / $300 | $559 / $118 |
| hike-25 market (rewarded, $500/day): best bid / ask $, depth within 3 ticks bid / ask | $8.4 k / $25.9 k; $151 k / $171 k (38.8 k s) | $31 k / $292 k; $203 k / $1.68 M |
| no-change market ($200/day): best bid / ask $, depth within 3 ticks | $5.9 k / $2.3 k; $41 k / $54 k | $107 k / $7 k; $403 k / $70 k |
| taker order size p50 / p90 / p99 | $15 / $585 / $7,505 (822 orders) | $9 / $43 / $9,826 (59) |
| taker orders per market-minute (five markets pooled) | 0.26 | 0.10 |
| median trade vs the level it hit; levels wiped | 0.3 %; 7 % | 0 %; 3 % |
| adds + cancels per market-minute; added size cancelled within 60 s | 34 + 29; 45 % | 16 + 16; 33 % |
| 1-s mid moves ≥ 2 ticks | 0.06 per hour (3 in 54 h) | 0 |
| taker direction (Yes frame), count / volume | 35 % / 43 % buys | 63 % / 12 % |

What it means:

- **Calm, deep, pinned at the tick.** The two live markets leave a 1-cent spread < 1 % of the time, and the queues
  at the touch of the rewarded market are $8-30 k on the bid and $26-290 k on the ask (the two days differ 10×, the
  resting size is somebody's rewards inventory, not flow): a median $15 order takes 0.3 % of the level. There is no adverse
  selection to speak of inside a day (3 two-tick moves in 54 hours) and nothing to earn from the spread either:
  a new order joins a queue worth hundreds of times the daily flow.
- **This is a rewards market, not a spread market.** $500/day is shared pro rata (quadratically in distance from
  mid) among $0.3-1.9 M of resting size within the 4.5 ¢ qualifying band (within 3 ticks: $320 k on 11-06, $1.9 M on
  11-07): a $10 k two-sided quote at the touch would earn $3-16 a day, 0.03-0.16 %/day on capital locked until
  2026-12-09, before the one scheduled jump on decision day. The inventory risk is the FOMC outcome, which the book does not warn about.
- Verdict: **not an active market-making venue; a rewards-yield position at best.** Stage 4 (rewards economics)
  should price it with the measured depth; the answer is a capital-return number, not a microstructure one.

## 4. The three most surprising numbers

1. **80 % of added size on btc_15m is cancelled at its level within 1 second** (95 % within 10 s; upper bound, but
   the 10-s figure cannot be far off). The 15-minute book is not a queue of patient orders, it is a continuously
   re-priced screen; queue position in the usual sense barely exists.
2. **The ladder's near-the-money strikes are wider than the tick 57-70 % of the time** (p50 1.6-2.1 ticks, p90
   3.3-4) while every other book we record is pinned at 1 tick. It is the only place in the batch where a spread
   is a decision.
3. **The last minute of a 15-minute window sees ~1,900 two-tick moves per hour and one side empty 83 % of the
   time**, i.e. a 2-tick jump every 2 seconds in a one-sided book, versus 600/h and 0 % in minute 0. The quoting
   window is minutes 0-9, not 0-14.

## 5. Caveats and follow-ups

- One weekday block and one Saturday morning; weekday US hours, FOMC weeks and high-vol days are not represented.
  Re-run `polymm shape` on every VPS day and watch whether the btc_15m minute profile and the ladder's near-money
  spread are stable before building the simulator on them.
- The laptop block has consumer stalls (quality report §4 item 4). Time shares and depth are fine inside strict
  windows; the add/cancel rates and the resting-time proxy are per exchange-ordered events and unaffected, but
  anything that needs sub-second timing (queue position, Binance lead) must use VPS days.
- The resting-time proxy attributes cancels at a level to every add that preceded them within the horizon
  (L2 book, no order ids): an upper bound. A size-aware one-to-one matcher would tighten it.
- Depth is measured over the 5 stored levels, so "within 5 ticks" is a lower bound in dense books (fed).
- Moneyness uses Binance where recorded and otherwise the ladder-implied BTC (0.5-crossing of the Yes mid, nearest
  expiry ≥ 1 h out); the 0.5 % buckets are coarse enough for that.
- Fill counts are per trade message; one taker order hitting several makers at one price is one order here (50 ms
  grouping), which the quality report's "11 fill rows per trade" problem does not affect.
- The 11-06 btc_15m `post` bucket (35 market-minutes after the window end, 2.3 orders/min, 83 % Yes buys) is
  settlement trading of the winning token, not market making; it is excluded from the live-window numbers.

## 6. Runtime

| day | input | shape | peak RSS |
|---|---|---|---|
| 2026-11-06 laptop block (11 h 15 min) | depth_1s 2.85 M rows, events_labeled 42 M, trades 61 k | 11.4 min alone (cancel proxy 5.6 min, flow 1.5 min, trade-vs-queue 0.5 min); 20 min with the test suite running beside it | 1.5-1.75 GB |
| 2026-11-07 VPS (2 h 20 min) | 0.3 M / 5 M / 12 k | 30 s | – |

Both on top of an existing replay; `polymm shape` runs `polymm quality` (and the replay) first when the strict
windows or the derived tables are missing.
