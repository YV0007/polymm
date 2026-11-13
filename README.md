# polymm: Market Making on Polymarket

A market-making framework for Polymarket's order book: a 24/7 multi-venue recorder,
order-book replay, fair value, adverse-selection markouts, a queue/fill model and an
event-driven simulator.

**Question:** does quoting both sides earn more in spread and rewards than it loses to
informed flow?

## In numbers

| | |
|---|---|
| Order-book rows recorded | **330 M** (9 days, 28.7 GB) |
| Feeds | Polymarket CLOB + **14** external feeds → **28** typed tables (~12 GB/day) |
| Maker fills analysed | **568 k** |
| Virtual queue orders simulated | **4.7 M** |
| Feed latency to server, p50 | Polymarket **12 ms** · Binance **128 ms** · order round trip **~50 ms** |
| Book reconstruction check | **6,201,126 / 6,201,126** rebuilt books match |

## Architecture

```text
Polymarket WS ──┐
14 ext. feeds ──┼──► Recorder (VPS, 24/7) ──► raw zstd + Parquet
REST poller ────┘                                   │
                                                    │ nightly pull
                                                    ▼
                                       Replay (L2 book per event)
                                          │                 │
                                          ▼                 ▼
                                     Fair value       Queue & fills
                                          │                 │
                                          ▼                 │
                                      Markouts              │
                                          │                 │
                                          └──► Simulator ◄──┘
                                                   │
                                                   ▼
                                                 Gate
```

| Layer | What it does |
|---|---|
| **Recorder** | Streams every book change and trade with our own receive timestamps. No historical book API exists, so this is the only source. |
| **Gap detection** | No sequence numbers, so the WS book hash is compared with REST every 60 s and re-checked after 2 s before a reset. |
| **Replay** | Rebuilds the L2 book event by event and validates it against REST snapshots. |
| **Fair value** | Log-odds blend of the empirical microprice and a BTC digital-option model, weight fitted walk-forward. |
| **Markouts** | Every public trade becomes a maker fill, marked out from 1 s to resolution. |
| **Queue model** | Virtual orders inserted into the replayed book track shares ahead per update: fill probability and toxicity by queue position. |
| **Simulator** | Our quotes in the replayed book with order-path latency, post-only rejects, cancels in flight, fees and rewards. |

## P&L decomposition

```math
\text{P\&L} = \underbrace{\sum_i s_i(m_i - P_i)}_{\text{spread}} + \underbrace{\sum_i s_i(m_i^{\tau} - m_i)}_{\text{adverse selection}} + \underbrace{\sum_i s_i(V_T - m_i^{\tau})}_{\text{inventory}} + \underbrace{R}_{\text{rewards}} - F
```

$s_i$ side, $P_i$ fill price, $m_i$ fair value at the fill, $m_i^{\tau}$ fair value $\tau$ later.
Edge is counted on spread + adverse selection + rewards only; inventory is shown, never counted.

## Design choices

| Choice | Why |
|---|---|
| Record raw frames before parsing | A parser bug becomes a re-run, not lost data. |
| zstd level 12 live | 9.4× compression at ~50 MB/s; level 19 gives 11.8× but at 2 MB/s, too slow live. |
| Our rebuilt book is the truth | Server best bid/ask fields disagree with the exact rebuilt book in ~0.4 % of frames. |
| Store one token per market | YES and NO mirror each other: −49.9 % rows, every rebuilt book checked equal. |
| One WebSocket per market family | A "slow consumer" disconnect hits one family, not the whole feed. |
| No big CPU step on the event loop | 100k-row conversions froze the loop 0.4–12 s and caused disconnects; now sliced + worker thread. |
| Fair value in log-odds | Prices near 0 or 1 move on a different scale; log-odds makes moves comparable. |
| Book read just before the taker's first consumed level | The book at receive time is already post-trade in 21 % of cases. |
| Latency as measured distribution, not a constant | The latency tail coincides with jumps, which is exactly when fills are toxic. |
| Walk-forward, lockbox, deflated Sharpe | Every parameter is chosen on past days and tested on unseen ones. |

## Findings so far (BTC 15-minute markets)

- Makers earn **+0.65 ¢** per share at the fill and give it back within **1 s** (**−0.24 ¢** at 60 s); the ~0.26 ¢ rebate keeps the average maker near zero.
- The loss is a race: in the 500 ms before our feed shows a BTC jump, takers already trade in its direction in **29–60 %** of 34.5 k jumps (13–23 % in the control).
- A jump guard in the simulator removes **~90 %** of the loss; no strategy passes the gate yet.

![Markout signature](research/adverse_selection/signature.png)

## Stack

Python 3.12 · asyncio/websockets · polars · DuckDB · PyArrow/Parquet · NumPy/Numba · Hetzner VPS · S3 bucket

```bash
uv sync
uv run polymm record      # recorder
uv run polymm replay      # rebuild books
uv run polymm markouts    # adverse selection
uv run polymm sim         # simulator
```

Research only, no live trading until the data shows an edge.
