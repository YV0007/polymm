# polymm

## Results

Strategy: an event-driven two-sided quoter at ±2–3 ¢ around the fair value, 10 shares a side. It re-prices on every book event and Binance tick. A jump guard pulls the threatened side for 1–3 s after a fast fair-value move, and quoting is reduce-only in the last 60 s.

Simulation conditions:
- the full recorded book, every update on our receive clock;
- a 75 ms order path;
- queue position tracked per update;
- the pessimistic fill model (the most conservative of four);
- taker fee $7\% \cdot p(1-p)$ with a 20 % maker rebate.

Parameters are chosen walk-forward on past days only. The last day is a lockbox, opened once.

| | Windows | P&L per 15-min window (USD) |
|---|---|---|
| Test, 2026-11-07 → 11-10 | 355 | **+0.68 ± 0.44** |
| Lockbox, 2026-11-11 | 96 | **+0.93 ± 0.98** |

| Day | 11-07 | 11-08 | 11-09 | 11-10 | 11-11 |
|---|---|---|---|---|---|
| USD per window | +1.61 | +1.30 | −0.49 | +0.55 | +0.93 |

Moving the fair value from a 1 s grid to every book event took the same strategy from −0.62 to +0.78 USD per window, a paired gain of +1.23 ± 0.92. That run used the matched fill model. Fills fell by two-thirds, and the fills kept were far less toxic.

## P&L decomposition

For fill $i$ with side $s_i$, price $P_i$, fair value $m_i$ at the fill, $m_i^{\tau}$ at $\tau$ later and settlement $V_T$:

```math
\text{P\&L} = \underbrace{\sum_i s_i(m_i - P_i)}_{①\ \text{spread}} + \underbrace{\sum_i s_i(m_i^{\tau} - m_i)}_{②\ \text{adverse selection}} + \underbrace{\sum_i s_i(V_T - m_i^{\tau})}_{③\ \text{inventory}} + \underbrace{R}_{④\ \text{rebates}} - F
```

USD per window, $\tau = 60$ s, pessimistic fills:

| | ① Spread | ② Adverse selection | ③ Inventory | ④ Rebate | Total |
|---|---|---|---|---|---|
| Test | +0.34 | −0.61 | +0.85 | +0.09 | **+0.68** |
| Lockbox | +0.39 | −0.97 | +1.40 | +0.11 | **+0.93** |

About 36 shares and 4 fills per window. Peak capital is about 130 USD.

## Architecture

```text
  RECORDING  ·  VPS, 24/7

  ┌──────────────────────────────┐      ┌──────────────────────────────┐
  │  polymm record               │      │  polymm-feeds record         │
  │                              │      │                              │
  │  Polymarket CLOB  WS + REST  │      │  Binance · Coinbase · OKX    │
  │  gap checks against REST     │      │  Bybit · Hyperliquid         │
  │  Binance spot                │      │  Deribit · Chainlink         │
  └───────────────┬──────────────┘      └───────────────┬──────────────┘
                  │                                     │
                  └──────────────────┬──────────────────┘
                                     ▼
                    ┌────────────────────────────────┐
  STORAGE           │  object storage                │
                    │  raw zstd + Parquet, hourly    │
                    └────────────────┬───────────────┘
                                     ▼
  MODELLING PIPELINE

  replay          L2 book rebuilt event by event
    ▼
  quality         clean windows only
    ▼
  fair value      order book + BTC digital option, in log-odds
    ▼
  markouts        adverse selection from 0.1 s to resolution
    ▼
  queue model     fill probability and toxicity by queue position
    ▼
  simulator       latency, cancels, fees, P&L decomposition
```

| Layer | Implementation |
|---|---|
| Recorder | Stores raw frames first, with our receive stamp in ns. No historical book API exists, so this is the only source. One WS per market family isolates slow-consumer disconnects. An idle watchdog reconnects silent sockets. |
| Gap detection | The Polymarket WS has no sequence numbers. The book hash is compared with REST every 60 s and re-checked after 2 s before a reset. External venues use their sequence chains (Binance `pu`/`U`, Bybit `u`, OKX `seqId`). |
| Compute | `ops/burst.sh` creates a 16-vCPU / 64 GB server, fetches from the bucket, runs, pulls results and deletes it. A TTL guards against forgotten servers. Cost: €0.38/h. |

Measured latency: Polymarket feed to VPS p50 11 ms, Binance feed p50 127 ms, VPS to CLOB order round trip p50 46–52 ms.

## Models

### Fair value

**Intuition.** Two estimates of the probability that BTC finishes above the strike:
- **The order book.** The mid price, nudged toward the side where the book leans.
- **An option model on BTC.** It reacts to BTC moves before the book does.

The fair value is the book, pulled part of the way toward the option model. Everything is in log-odds, $x = \ln\frac{p}{1-p}$, so a move near 1 ¢ or 99 ¢ counts as much as one near 50 ¢.

```math
x_t = x^{\text{book}}_t + w\left(x^{\text{btc}}_t + o_t - x^{\text{book}}_t\right)
```

- $x_t$: fair value at time $t$, in log-odds, recomputed on every book update and every BTC tick.
- $w$: how far to trust the option model over the book. It is fitted on past days only.
- $o_t$: a slow-moving average of the gap between the two estimates. It removes the model's persistent level error, so only its moves count.

**Book estimate.** The mid, corrected by how the price moved on average after the same spread and imbalance in the past:

```math
x^{\text{book}}_t = \operatorname{logit}(\text{mid}_t) + \text{adj}\left[\text{spread}_t,\ \text{imbalance}_t\right]
```

Here imbalance = bid size / (bid size + ask size) at the best prices.

**Option estimate.** The market pays out if BTC's settlement price ends above the strike. That is a digital option:

```math
x^{\text{btc}}_t = \operatorname{logit}\,\Phi\!\left(\frac{\ln\left(\mathbb{E}_t[A]/K\right)}{\sigma_t\sqrt{\tau_{\text{eff}}}}\right),
\qquad
\tau_{\text{eff}} =
\begin{cases}
\tau - 40\ \text{s} & \tau > 60\ \text{s} \\
\tau^{3} / (3 \cdot 60^{2}) & \tau \le 60\ \text{s}
\end{cases}
```

- $\Phi$: standard normal CDF.
- $K$: the strike (BTC price at the window's start).
- $A$: the settlement price, an average of the Chainlink BTC price over the last 60 s.
- $\mathbb{E}_t[A]$: its expected value, with the part of the average already observed filled in.
- $\sigma_t$: BTC volatility, an exponentially weighted average of 1-minute returns with the time-of-day pattern removed.
- $\tau$: time to expiry. $\tau_{\text{eff}}$ is the time-equivalent of the uncertainty left. Averaging over the last minute removes part of it, and inside that minute the observed part is fixed.

### Markouts

**Intuition.** After a maker is filled, watch where the fair value goes. If it moves against the maker, the taker knew something. The size of that move is the adverse selection.

```math
M_i(\tau) = s_i\left(m_{t_i+\tau} - P_i\right)
```

- $i$: one fill.
- $s_i$: the maker's side, $+1$ bought or $-1$ sold.
- $P_i$: fill price.
- $t_i$: fill time.
- $m_{t}$: fair value at time $t$.
- $\tau$: horizon, from 0.1 s to the market's resolution.

Every public trade counts as a fill for the resting maker. $m$ is read just before the trade: at the moment we receive it, the book has already moved in 21 % of cases.

### Queue model

**Intuition.** On a 1-tick book everyone quotes the same price, so what matters is the place in line. Orders at the back fill only when a large trade sweeps the whole level, which is exactly when the price is about to move.

A simulated order joins the back of its price level. The shares ahead of it shrink as trades and cancellations happen; four different assumptions are tried for where cancellations sit in the line. It fills when a trade reaches it. Each place in line, $x$ = shares ahead, gets a value:

```math
V(x) = P(\text{fill} \mid x)\left(h - \mathbb{E}\left[a \mid \text{fill}, x\right]\right)
```

- $P(\text{fill} \mid x)$: chance of being filled from place $x$.
- $h$: the half-spread earned on a fill.
- $a$: how far the fair value moves against the maker afterwards.

### Simulator

The strategy runs inside the recorded book, as if it had been live:
- **Quoting.** Buy $h$ below fair and sell $h$ above. After a sudden fair-value move, pull the side about to be hit for 1–3 s. In the last minute, only reduce the position.
- **Execution.**
  - Orders take about 75 ms to arrive, with random delay and occasional bursts.
  - Orders that would cross the spread are rejected.
  - A cancel can arrive too late, and the order still fills.
  - Takers pay a fee of $7\% \cdot p(1-p)$ per share, where $p$ is the price. Makers get 20 % of it back.
- **Validation.**
  - Settings are chosen on past days only.
  - The last day is held back and tested once.
  - The Sharpe ratio is adjusted for how many settings were tried.

## Repository

```text
src/polymm/        recorder, replay, quality, fair/, markouts/, queue/, sim/, rewards/, feeds/
tests/             133 tests
research/          one report per stage with plots
docs/DECISIONS.md  decision log: what was chosen, measured and rejected
ops/               deploy, systemd units, nightly pull, burst compute
```

Python 3.12, asyncio, polars, DuckDB, PyArrow, NumPy/Numba, uv.

```bash
uv sync
uv run polymm record          # Polymarket recorder
uv run polymm-feeds record    # external venues
uv run polymm replay          # rebuild books, validate against REST
uv run polymm fair            # walk-forward fair value
uv run polymm queue           # queue position, fill probability, toxicity
uv run polymm sim             # strategy, walk-forward, decomposition
```
