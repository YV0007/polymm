# polymm

Market-making research on Polymarket's CLOB. The question is whether two-sided quoting earns more in spread and rebates than it loses to informed flow. It has to be answered on recorded data before any capital goes in.

The current target is the BTC 15-minute Up/Down markets: a binary payoff on an external price that is discovered elsewhere first. That is the hardest case for a passive maker and a clean test of adverse selection.

## Status (2026-11-14)

**No positive market-making edge yet. The gap is now small and well understood.**

Strategy: an event-driven two-sided quoter around a fair value, 10 shares a side at ±2–3 ¢. A jump guard pulls the threatened side for 1–3 s after the fair value moves ≥ 0.5–1 ¢ in 0.5 s, and quoting is reduce-only in the last 60 s. Simulated on the replayed book with a 75 ms order path, walk-forward parameter selection, four days of test and one held-out lockbox day.

| Version | USD per 15-min window (test, 355 windows) |
|---|---|
| Quoter, fair value on a 1 s grid | −4.47 ± 1.14 |
| + jump guard, reduce-only near expiry | −0.62 ± 0.78 |
| + fair value updated on every book event, one decision per instant | +0.78 ± 0.48 |
| &nbsp;&nbsp;of which inventory ③ | +0.90 ± 0.41 |
| &nbsp;&nbsp;**of which market making ① + ② + ④** | **−0.12** (pessimistic fills: **−0.18 ± 0.21**) |

The positive total comes entirely from inventory, which is not counted as edge: it scales with risk held, not volume, and shows no skill at resolution. Per filled share under the pessimistic fill model the split is: spread +0.93 ¢, adverse selection at 60 s −1.68 ¢, rebate +0.25 ¢. Gate B fails narrowly. On totals the pessimistic 95 % lower bound is −0.18 and the deflated Sharpe is 0.005. On ① + ② + ④ the 95 % interval is −0.60 … +0.24. Separating the mean from zero would need about 526 windows; 355 are available.

## P&L decomposition

For fill $i$ with side $s_i$, price $P_i$, fair value $m_i$ at the fill, $m_i^{\tau}$ at $\tau$ later and settlement $V_T$:

```math
\text{P\&L} = \underbrace{\sum_i s_i(m_i - P_i)}_{①\ \text{spread}} + \underbrace{\sum_i s_i(m_i^{\tau} - m_i)}_{②\ \text{adverse selection}} + \underbrace{\sum_i s_i(V_T - m_i^{\tau})}_{③\ \text{inventory}} + \underbrace{R}_{④\ \text{rebates}} - F
```

Edge is ① + ② + ④ at $\tau = 60$ s under the pessimistic fill model. ③ is reported beside it and never counted.

## Findings

**The market pays a passive maker about zero.** Over 328 k public fills the average resting maker captures +0.70 ¢ and gives back 0.91 ¢ by 60 s, which nets +0.04 ¢ per share after the rebate. The book sits at a 1-tick spread 98 % of the time with 170–420 shares at the touch. The fair value moves more than one tick per second (SD 1.3–1.5 ¢/s) and jumps ≥ 1 ¢ every 8–12 s. No crypto Up/Down market has a liquidity reward pool.

![Maker markout signature](research/adverse_selection/signature.png)

**Adverse selection is a sub-second race.** 79–87 % of the adverse move after our fills lands within 1 s. In 29–60 % of BTC-driven fair-value jumps, takers trade in the jump's direction in the 500 ms before our feed shows it, against 13–23 % in the opposite-direction control. The rate rises with jump size.

**Most of "takers are 1 s ahead" was our own lateness.** The first simulator re-priced on a 1 s grid. Moving the fair value on every change of the touch cut adverse selection per share from −4.1 ¢ to −1.5 ¢ and fills by two-thirds. Swept-through fills fell from 46 % to 22 % of volume. The value of a "fair value 1 s earlier" oracle dropped from +2.24 to +0.65 USD per window.

**Speed is not the remaining lever.** Oracles on the current stream, rung 3, 451 windows, USD per window, gain vs base ± paired SE:

| Oracle | Gain |
|---|---|
| BTC input 500 ms earlier, book at true time | +0.21 ± 0.17 |
| Zero order latency | +0.31 ± 0.41 |
| Front of the queue | +0.14 ± 0.71 |
| Instant cancels | −0.34 ± 0.33 |
| Whole fair-value stream 1 s earlier | +0.65 ± 0.48 |
| Whole fair-value stream 5 s earlier | +5.84 ± 0.81 |

None of the execution levers is distinguishable from zero. A faster BTC input alone is capped near +0.2. What remains is predicting the book's own next few seconds, so the problem is order-flow and cross-venue information.

## Architecture

```text
 VPS (Hetzner, 24/7)                                    Object storage (fsn1)
 ├─ polymm record        Polymarket CLOB WS, REST       ┌──────────────────────┐
 │                       gap checks, Binance spot   ──► │ raw zstd + Parquet,  │
 └─ polymm-feeds record  12 WS + 3 REST polls:          │ hourly upload,       │
                         Binance perp/spot, Coinbase,   │ sha256-verified      │
                         OKX, Bybit, Hyperliquid,       └──────────┬───────────┘
                         Deribit, Chainlink (+60 s TWAP)           │
                                                                   ▼
 Research (laptop for slices, burst server by the hour for full days)
   replay ──► quality ──► fair value ──► markouts ──► queue/fill model ──► simulator ──► gate
   (L2 book    (clean       (log-odds,     (② by τ,      (virtual orders,      (event engine,
    per event)  windows)     walk-forward)  signature)    fill prob, toxicity)  latency, fees)
```

| Layer | Implementation |
|---|---|
| Recorder | Raw frames first, our receive stamp in ns. No historical book API exists, so this is the only source. One WS per market family isolates slow-consumer disconnects. An idle watchdog reconnects silent sockets. |
| Gap detection | The WS has no sequence numbers. The book hash is compared with REST every 60 s and re-checked after 2 s before a reset. External feeds use venue sequence chains (Binance `pu`/`U`, Bybit `u`, OKX `seqId`). |
| Replay | L2 book rebuilt event by event: 6,201,126 of 6,201,126 rebuilt books match REST snapshots. The rebuilt book is treated as truth, since server best bid/ask disagrees with it in ~0.4 % of frames. |
| Fair value | Log-odds blend of a Stoikov-adjusted book price and a BTC digital-option model anchored to the book. Weights are fitted walk-forward. Recomputed on every touch change and every Binance tick. |
| Markouts | Public trades become maker fills (YES-equivalent). The reference is the book just before the taker's first consumed level, because at receive time the book is already post-trade in 21 % of cases. |
| Queue model | Virtual orders in the replayed book track shares ahead per update, under four cancel models. Output: fill probability and toxicity by queue position. |
| Simulator | Compiled event engine on the queue model, with one decision per receive timestamp. Includes order-path latency as a measured distribution, post-only rejects, cancels in flight, the fee schedule $7\%\cdot p(1-p)$ with a 20 % maker rebate, and oracle switches. |
| Validation | Walk-forward parameter choice, a lockbox day opened once, a plateau check on neighbouring settings, deflated Sharpe across all trials, and a pessimistic fill model for the headline. |
| Compute | No standing research server. `ops/burst.sh` creates a 16-vCPU/64 GB box, fetches from the bucket, runs, pulls results and deletes it. A cloud-init TTL kills forgotten servers. Cost: €0.38/h. |

Measured latency: Polymarket feed to VPS p50 11 ms, Binance feed (Tokyo) p50 127 ms, VPS to CLOB order round trip p50 46–52 ms.

## Direction

1. **Fair value as a term structure.** Model the forecast curve
   ```math
   \hat{x}_t(h) = \mathbb{E}\left[x^{\text{ref}}_{t+h} \mid \mathcal{F}_t\right], \qquad h \in \{0.1, 0.25, 0.5, 1, 2, 5, 30, 60\}\ \text{s}
   ```
   in log-odds on our receive clock. Quotes centre on $\hat{x}_t(h_{\text{rest}})$, where $h_{\text{rest}}$ is order latency plus expected resting time (0.5–2 s here). The short end drives cancels. The long end is calibrated against resolution. The nowcast comes first (convergence given what is already known), then the forecast.
2. **Cross-venue price discovery.** BTC perp and spot books, trades and liquidations from five venues, the Chainlink settlement stream and Deribit options have been recorded since 2026-11-13 (~105 GB/month raw, 28 typed tables). Next is a lead–lag study, then next-second $R^2$ against the break-even $R^2$ implied by a noisy-oracle sweep.
3. **Cheap rules into the ladder.** No price-improving or fresh orders into a moving fair value, and stand aside on a book 2 ¢ or wider.
4. **Market screen.** Score other markets on the factors that set a maker's baseline: average-maker net, pre-emption vs an external driver, jump rate per tick, and rewards per unit of capital.
5. **Live engine** in Rust (official SDK) only after Gate B passes, with paper trading and a sim-vs-live comparison first.

## Repository

```text
src/polymm/        recorder, replay, quality, fair/, markouts/, queue/, sim/, rewards/, feeds/
tests/             133 tests (replay identity, queue/pool equivalence, sim reproduction, parsers)
research/          one report per stage with plots
docs/DECISIONS.md  append-only decision log: what was chosen, measured and rejected
docs/DATA_PLAN.md  datasets, schemas, verified API quirks
ops/               deploy, systemd units, nightly pull, burst compute
```

Python 3.12, asyncio, polars, DuckDB, PyArrow, NumPy/Numba, uv. About 20 k lines.

```bash
uv sync
uv run polymm record          # Polymarket recorder
uv run polymm-feeds record    # external venues
uv run polymm replay          # rebuild books, validate against REST
uv run polymm fair            # walk-forward fair value
uv run polymm markouts        # adverse selection
uv run polymm queue           # queue position, fill probability, toxicity
uv run polymm sim             # strategy ladder, walk-forward, Gate B
uv run polymm diagnose        # fill anatomy, oracles, race
```

Research only. Nothing trades live until the edge is positive out of sample.
