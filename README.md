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

  replay        L2 book rebuilt from WS deltas, event by event;
                each change labelled add / cancel / fill.
                6,201,126 of 6,201,126 rebuilt books match REST.
    ▼
  quality       only clean windows: no reconnect, no confirmed gap,
                no crossed book, YES = 1 − NO mirror holds.
    ▼
  fair value    in log-odds, updated on every book event and BTC tick
                x      = x_book + w · (x_btc + offset − x_book)
                x_book = logit(mid) + adj[spread, imbalance]
                x_btc  = logit Φ( ln(E[A]/K) / σ·√τ_eff )
                A  = Chainlink 60 s settlement average, its known
                     part already in E[A]
                τ_eff = τ − 40 s,  or τ³ / (3·60²) inside the last
                     minute (variance left in the average)
                σ  = EWMA of de-seasonalised 1-min BTC returns
                offset = EWMA anchor of x_book − x_btc;
                w fitted walk-forward, never on the test day
    ▼
  markouts      every public trade as a maker fill, YES-equivalent;
                s·(m(t+τ) − P) for τ = 0.1 s … resolution, with m
                read just before the taker's first consumed level
    ▼
  queue model   virtual orders join the back of each level; shares
                ahead drain on trades and cancels under 4 cancel
                models → P(fill | position), toxicity by position
    ▼
  simulator     quotes at fair ± h, jump guard, reduce-only at expiry;
                75 ms order path (lognormal + bursts), post-only
                rejects, cancels in flight, fee 7%·p(1−p), 20% rebate;
                walk-forward settings, lockbox day, deflated Sharpe
```

| Layer | Implementation |
|---|---|
| Recorder | Stores raw frames first, with our receive stamp in ns. No historical book API exists, so this is the only source. One WS per market family isolates slow-consumer disconnects. An idle watchdog reconnects silent sockets. |
| Gap detection | The Polymarket WS has no sequence numbers. The book hash is compared with REST every 60 s and re-checked after 2 s before a reset. External venues use their sequence chains (Binance `pu`/`U`, Bybit `u`, OKX `seqId`). |
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
