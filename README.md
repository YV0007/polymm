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
  RESEARCH  ·  burst servers by the hour

      replay          L2 book rebuilt event by event
        ▼
      quality         clean windows, gap flags
        ▼
      fair value      log-odds, walk-forward
        ▼
      markouts        adverse selection by horizon
        ▼
      queue model     fill probability, toxicity by queue position
        ▼
      simulator       latency, fees, P&L decomposition
```

| Layer | Implementation |
|---|---|
| Recorder | Stores raw frames first, with our receive stamp in ns. No historical book API exists, so this is the only source. One WS per market family isolates slow-consumer disconnects. An idle watchdog reconnects silent sockets. |
| Gap detection | The Polymarket WS has no sequence numbers. The book hash is compared with REST every 60 s and re-checked after 2 s before a reset. External venues use their sequence chains (Binance `pu`/`U`, Bybit `u`, OKX `seqId`). |
| Replay | Rebuilds the L2 book event by event. 6,201,126 of 6,201,126 rebuilt books match REST snapshots. |
| Fair value | Log-odds blend of a Stoikov-adjusted book price and a BTC digital-option model anchored to the book. Weights are fitted walk-forward. Recomputed on every touch change and Binance tick. |
| Markouts | Public trades become maker fills (YES-equivalent). Each is marked against the book just before the taker's first consumed level, from 0.1 s to resolution. |
| Queue model | Virtual orders in the replayed book track shares ahead per update, under four cancel models. Output: fill probability and toxicity by queue position. |
| Simulator | Compiled event engine on the queue model, one decision per receive timestamp. Covers order-path latency, post-only rejects, cancels in flight, fees and rebates, plus oracle switches for valuing each lever. |
| Validation | Walk-forward parameter choice, a lockbox day, a plateau check on neighbouring settings, deflated Sharpe across all trials. |
| Compute | `ops/burst.sh` creates a 16-vCPU / 64 GB server, fetches from the bucket, runs, pulls results and deletes it. A TTL guards against forgotten servers. Cost: €0.38/h. |

Measured latency: Polymarket feed to VPS p50 11 ms, Binance feed p50 127 ms, VPS to CLOB order round trip p50 46–52 ms.

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
