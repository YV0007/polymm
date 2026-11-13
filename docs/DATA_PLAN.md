# Data & Collection Plan — Polymarket MM Research

Status: APPROVED 2026-11-05 (decisions in §4). Date: 2026-11-05. Covers the next 1–2 months (Phases 1–4, groundwork for 5).

Three blocks: **1. What data** (map, contents, purpose, source) · **2. How we collect it** (modes, architecture, build plan) · **3. How we store it** (layout, schemas, formats, scaling).

Every API detail below was checked against docs.polymarket.com on 2026-11-04. Items marked `VERIFY` are documented ambiguously and must be confirmed against live data in the first hour of recording.

---

## 1. Data map

### 1.1 Vocabulary (so the table reads cleanly)

| Term | Meaning on Polymarket |
|---|---|
| **market** | One binary question. Identified by `conditionId` (on-chain) and Gamma `id`. Has two outcome tokens. |
| **token / asset_id** | YES or NO outcome token id (a big integer string). **All CLOB and WS endpoints use token ids, not market ids.** |
| **event** | Group of markets (e.g. "Who wins the election" = one market per candidate). Mutually exclusive groups use `negRisk=true`, so Σ p ≈ 1 across the group. |
| **L2 book** | Aggregated size per price level: `[{price, size}, …]` per side. You see *how much* rests at 0.52, not *whose* orders or their queue order. Polymarket exposes only L2; L3 (per-order) does not exist publicly. |
| **snapshot** | The full L2 book at an instant. |
| **delta** | One level's new total size after a change. `size=0` means the level is gone. A delta is *not* "+10 shares"; it is "this level now holds 40". |
| **trade** | One taker order matched against resting orders. A sweep across several levels = several trade messages sharing the same `transaction_hash`. |
| **hash** | Fingerprint of book state. Same book ⇒ same hash. Used for resync detection since **there are no sequence numbers**. |
| **ts_exch / ts_recv** | Exchange timestamp inside the message (ms) vs our local receive time (ns). Both always stored. Their difference is feed+network latency; never mix them. |
| **tick size** | Min price increment. 0.01 normally; 0.001 on some markets near 0/1. Changes are announced on the WS. |

### 1.2 The datasets

Mode: **S** = stream (must be captured live, cannot be backfilled) · **P** = periodic REST poll · **B** = backfill once, re-runnable · **O** = ours (derived from the recorder itself).

| ID | Dataset | What is inside (fields) | Why we need it (phase) | Source | Mode |
|---|---|---|---|---|---|
| **D1** | **Market metadata** | `id, slug, question, conditionId, clobTokenIds[2], outcomes[2], eventId, negRisk, orderPriceMinTickSize, orderMinSize, endDate, closed, acceptingOrders, feesEnabled, feeSchedule{rate, exponent, takerOnly, rebateRate}, rewardsMinSize, rewardsMaxSpread, volume24hr, liquidity, tags/category` | Universe selection; token-id lookup; tick size & min size for the simulator (P5); fees → F and rebates in ④ (P4); reward params → ④ (P4); `endDate` → capital lock and resolution horizon (P4–5) | Gamma `GET https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=…&offset=…` (also `/markets/keyset`, `/events`) | P, daily + at recorder start |
| **D2** | **Rewards config** | per market: `rewards_min_size, rewards_max_spread, rewards_config[{rate_per_day, start_date, end_date, total_rewards}]` | `rate_per_day` = size of the daily pool → the only way to value a quote in $ (P4) | CLOB `GET https://clob.polymarket.com/rewards/markets/multi` (`VERIFY` exact path; SDK `list_reward_markets()`) | P, daily |
| **D3** | **Book snapshot** | `market (conditionId), asset_id, bids[{price,size}], asks[{price,size}], hash, timestamp(ms)`; REST adds `tick_size, neg_risk, min_order_size, last_trade_price`. REST: best levels are the **last** elements of each array. | Starting state for book reconstruction; depth/spread (P2); resync anchor after any gap (P1) | WS `book` event (on subscribe + occasionally) and REST `GET https://clob.polymarket.com/book?token_id=` / `POST /books` (≤500) | S + P (REST every 60 s and on every reconnect) |
| **D4** | **Book deltas** | `market, timestamp(ms), price_changes[{asset_id, price, size, side(BUY/SELL), hash, best_bid, best_ask}]`. `size` = new total at that level (`VERIFY`). | **The core dataset.** Queue position & depth over time (P2); microprice/mid at every instant for markouts (P3); reward-sampling of the book (P4); event-level replay for honest fills (P5). Cannot be backfilled anywhere. | WS `price_change` | S |
| **D5** | **Trades (live)** | `market, asset_id, price, size, side, fee_rate_bps, timestamp(ms), transaction_hash`. `side` = taker side (`VERIFY`: BUY means taker bought YES-token). | Markouts: fill price vs fair value τ later (P3); flow rate λ and size distribution (P2); which deltas were fills vs cancels (P5); `fee_rate_bps` → F and rebate (P4) | WS `last_trade_price` | S |
| **D6** | **Trades (historical)** | `timestamp(s), price, size, usdc_size, side, proxy_wallet, transaction_hash, outcome, outcome_index, token_id, condition_id`. `taker_only=true` ⇒ one row per fill. **3 years back** per condition. | Long-horizon flow stats before our recording is long enough (P2); sweep detection via shared `transaction_hash`; wallet-level toxicity: which wallets' fills have bad markouts → future "z" signal (P3+) | Data API `GET https://data-api.polymarket.com/v2/trades?condition=<id>&taker_only=true&limit=1000&cursor=…` (cursor pagination, no offset) | B |
| **D7** | **Tick size changes** | `market, asset_id, old_tick_size, new_tick_size, timestamp` | Simulator must know legal prices at every time (P5) | WS `tick_size_change` | S |
| **D8** | **Price history (coarse)** | `[{timestamp, price, resolution_seconds}]`; 1-min kept ≥7 d, 5-min ≥60 d, 30-min ≥90 d, 3h/12h permanent; explicit range ≤15 days per call | Jump frequency/size over months (P2); sanity check of our own mid series. **Not** for markouts (too coarse). | Data API `GET /v2/prices-history?token_id=&start=&end=&bucket_seconds=` | B |
| **D9** | **Resolutions** | winning outcome, resolution time, status | V_T for term ③; label every recorded market with its final value (P4–5) | Data API `GET /v2/resolutions?condition=` + WS `market_resolved` (needs `custom_feature_enabled: true`) | B + S |
| **D10** | **Events** | `id, slug, title, negRisk, markets[]`, end dates | Neg-risk groups → Σp = 1 constraint for fair value (P3); grouping for portfolio view (P5) | Gamma `GET /events?id=` | P, daily |
| **D11** | **Open interest** | per condition: OI in $ | Capital in market, crude proxy for "importance"; optional | Data API `GET /v2/oi?condition=` | P, daily |
| **D12** | **Recorder health** | `ts, conn_id, msgs_total, msgs_by_type, reconnects, gaps_detected, hash_mismatches, queue_depth, write_latency_ms, bytes_written` | Data you cannot audit is data you cannot trust. Every analysis later starts with "was the feed healthy here?" | ours | O, every 10 s |
| **D13** | **Raw WS frames** | verbatim JSON text + `ts_recv_ns` + `conn_id` | Insurance. Any parser bug or schema misunderstanding is recoverable by replaying raw. zstd-19, hourly files, ~50–120 MB/day. | ours | O, continuous |

**Later, not now:** external reference prices (Binance BTC/ETH spot via WS) for the crypto-threshold markets' fair value model (P3+). Same recorder design, different `Source` class — this is one reason the collector is built pluggable.

### 1.3 What each dataset is *for*, in one picture

```
D1/D2/D10 (metadata)  ──►  which markets, what tick, what fees, what rewards pool
D3 + D4   (book)      ──►  book state at every instant  ──►  mid, microprice, depth, queue
D5 / D6   (trades)    ──►  fills: when, price, size, taker side
D9        (resolution)──►  V_T

markout(τ) for fill i  =  s_i · ( m(t_i + τ) − P_i )      needs D4 (m) + D5 (P_i, s_i)
reward value of quote  =  f(book samples, rewardsMaxSpread, rate_per_day)   needs D4 + D1 + D2
simulator fill         =  queue(D4) ∧ trade(D5) at my level                  needs D4 + D5 + D7
```

### 1.4 Historical vs live availability (the asymmetry)

| | Backfillable? | From where |
|---|---|---|
| Trades | Yes, 3 years | Data API D6 |
| Coarse prices | Yes, months | Data API D8 |
| Resolutions, metadata | Yes | D1, D9, D10 |
| **Book snapshots / deltas** | **No** (official). Third-party: Tardis from 2026-05-25 (free: 1st of each month), Telonex from 2025-10-11 (paid) | **our recorder** |

Decision: record our own, use Tardis' free 2026-12-01 day only as a validator for our recorder.

---

## 2. Collection

### 2.1 Three collection modes, three programs

| Mode | Program | Runs | What it handles |
|---|---|---|---|
| Stream | `recorder` | 24/7 as a service | D3(WS), D4, D5, D7, D9(WS), D12, D13 |
| Poll | inside `recorder` as tasks | 60 s / daily | D3(REST), D1, D2, D10, D11 |
| Backfill | `backfill` CLI | on demand, idempotent | D6, D8, D9(REST) |
| Universe | `UniverseWatcher` inside `recorder` | every 60 s | turns the *rules* in `config.yaml` (market families) into the live token set; subscribes/unsubscribes on the open connection |

### 2.2 Recorder architecture

Five stations in a line, plus one side loop.

| # | Station | Does | Must be |
|---|---|---|---|
| ① | `WsSource` (listener) | Keeps the WebSocket open. On each message: stamp `ts_recv_ns`, put raw text on the queue. On disconnect: reconnect with backoff, enqueue a `RECONNECT` marker. | Never blocked. Does no parsing or disk I/O. |
| ② | `asyncio.Queue(maxsize=50k)` | Buffer between fast arrivals and slower processing. | Bounded, so a stuck downstream is visible in health, not a memory leak. |
| ③ | Interpreter (`Dispatcher` → `RawSink`, `Parser`, `BookState`) | Pull one message: write raw text to `jsonl.gz`; parse JSON → typed record; if delta, apply to in-memory book for that token and remember the hash; append record to its table buffer. | Raw written before parsing (insurance). |
| ④ | `ParquetSink` (writer) | Every 5 s or 10k rows: write a buffer as one Parquet file. Runs in a thread. Writes `.tmp`, then renames. | Slow is fine; never on the event loop; never leaves a half file. |
| ⑤ | Side loop: `RestPoller` + `Health` | Every 60 s: `POST /books` for all tokens, compare each hash with `BookState`. Match → nothing. Mismatch → log gap, reset that book from REST, emit a `snapshot_rest` row. Every 10 s: write counters (msgs/s, reconnects, gaps, queue depth, write latency). | Independent of the stream it audits. |

```
 Polymarket WS
      │  raw text
      ▼
 ① WsSource          stamp ts_recv, enqueue
      │
      ▼
 ② Queue (50k)       buffer for bursts
      │
      ▼
 ③ Interpreter
    ├─► RawSink       raw text → jsonl.gz
    ├─► Parser        json → typed record
    ├─► BookState     apply delta, keep hash
    └─► Buffers       one per table
      │
      ▼
 ④ ParquetSink       flush every 5 s / 10k rows
                     (thread, atomic rename)

 ⑤ Side loop
    RestPoller 60 s ─► GET /books ─► hash == BookState.hash ?
         yes → nothing
         no  → log gap, reset book, emit snapshot_rest → Buffers
    Health 10 s   ─► counters → health table + log
```

Why a pipeline rather than one function: each station has a different speed and failure mode. The listener must never block (or timestamps lie). The writer may be slow. The auditor must be independent of the stream it audits. Separating them lets each be tested, replaced (S3 for disk, Binance for Polymarket) and scaled (writer into its own process) on its own.

### 2.3 Components (pluggable, OOP) and the reason each exists

| Class | Responsibility | Why it is its own class |
|---|---|---|
| `Source` (ABC) → `PolymarketWsSource`, later `BinanceWsSource` | Connect, subscribe, heartbeat (`PING` every 10 s), yield `(ts_recv_ns, raw_text)`. Reconnect with exponential backoff; on reconnect emit a `RECONNECT` marker. | New venue = new subclass, nothing else changes. Keeps I/O separate from meaning. |
| `Parser` → `PolymarketMarketParser` | raw JSON → typed `Record` dataclasses (`BookSnapshot`, `BookDelta`, `Trade`, `TickChange`, `Resolved`). Unknown `event_type` → logged, kept in raw, dropped from tables. | One place that knows the message format. When Polymarket changes a field, one file changes. |
| `BookState` | Local L2 book per token: apply snapshot, apply delta, compute hash-independent sanity (crossed book? negative size?), store last hash. | Needed for gap detection (no seq numbers) and gives best bid/ask/microprice for health. Reused as-is by the simulator in P5. |
| `Sink` (ABC) → `ParquetSink`, `RawJsonlSink`, later `S3Sink` | Accept records, buffer, flush atomically (write `.tmp`, rename). | Swap local disk for S3 without touching the recorder. Atomic rename = a crash never leaves a half-written Parquet file. |
| `UniverseWatcher` | Every 60 s: for each family rule in config, compute the slugs that should be live now (15m: current + next 2 windows; daily ladder: today + next 2 days; fixed: as listed), resolve slug → `clobTokenIds` via Gamma, `subscribe` new tokens, `unsubscribe` tokens whose market ended >10 min ago, write every change to the `markets` table. | Rolling markets (BTC 15m) get new token ids every 15 minutes; a static token list cannot record them. Subscribing before the window opens captures book formation. |
| `RestPoller` | Every 60 s `POST /books` for all tokens; compare hash with `BookState`; on mismatch → emit `snapshot_rest`, reset book, count a gap. | Silent drops on the WS are otherwise invisible. 60 s bounds how long a wrong book can live. |
| `Health` | Counters → `health` table + log line. | Audit trail. |
| `Recorder` | Owns the event loop, wires the above, handles SIGTERM (flush everything, then exit). | Composition root. Only file that knows about all others. |
| `Config` (pydantic) | `config.yaml` → typed settings: token list, paths, flush params, poll intervals. | Validated at start; typos fail loudly, not at 3 am. |

### 2.4 Key design decisions and why

1. **Reader does one thing: stamp and enqueue.** `ts_recv_ns = time.time_ns()` is taken *before* any parsing. Parsing, hashing, writing happen downstream. Otherwise your "latency" measurement includes your own code.
2. **Bounded queue, not unbounded.** If the writer stalls, `queue.put` blocks and health shows `queue_depth` at max → you see the problem. Unbounded = silent memory growth = OOM at 4 am.
3. **Writer flushes in a thread (`asyncio.to_thread`).** Parquet encoding + gzip + fsync can take tens of ms; on the event loop that delays stamping the next message.
4. **Raw first.** Raw frames hit disk before any interpretation. Tables are a *view* on raw. A bug in `Parser` costs a re-run, not data.
5. **Reconnect ⇒ resubscribe ⇒ REST snapshot ⇒ mark gap.** A reconnect without a fresh snapshot means every delta afterwards is applied to a stale book, and nothing tells you.
6. **Hash check every 60 s, not continuously.** Continuous local-hash recomputation would need Polymarket's exact hash algorithm (undocumented). Comparing their REST hash with their last WS hash needs nothing.
7. **Config holds rules, not tokens.** For rolling markets a token list is stale in 15 minutes. Config names *families* (`btc-updown-15m`, `bitcoin-above-on-{date}`, fixed slugs); `UniverseWatcher` resolves them to tokens continuously and logs every subscribe/unsubscribe to the `markets` table, so a run is still reproducible from the data.
8. **Dataclasses for records, not dicts.** Typos in field names fail at construction. Schema lives in code, Parquet schema is derived from it — one source of truth.
9. **asyncio, single process.** 20–40 tokens at <1k msgs/s is far below one core. Multiprocessing now = complexity without benefit. The queue boundary is where a second process would go later.
10. **Idempotent backfill.** `backfill` writes `dt=`-partitioned files keyed by `(condition_id, cursor_page)`; re-running overwrites identical files. Safe to retry after any failure.

### 2.5 Scaling path (what changes when it must)

| Trigger | Change | Untouched |
|---|---|---|
| >500 tokens | Several `WsSource` instances, one connection each, same queue | Parser, Sink, schemas |
| Writer can't keep up | Replace in-process queue with Redis/Kafka; `Recorder` splits into reader process + writer process | Source, Parser, schemas |
| Laptop can't stay up | Same code on a VPS under `systemd`; nightly `rsync` of `data/` | everything |
| Disk full | `S3Sink` instead of local `ParquetSink`; analysis reads `s3://` with the same DuckDB/polars code | everything |
| Add Binance for fair value | `BinanceWsSource` + `BinanceParser`, new table | Recorder, sinks |

### 2.6 Market batch (approved 2026-11-05)

| Slot | Family rule | Live tokens | Why |
|---|---|---|---|
| BTC 15m | `btc-updown-15m-{ts}`, `ts` = window start (unix, multiple of 900); keep current + next 2 windows | 2 per window, ~6 | Most toxic regime; external fair value (spot BTC). Tick 0.01, min 5, taker fee 7%·p(1−p), rebate 20%, rewards 50 sh / 4.5¢. Resolves on Chainlink TWAP vs open. |
| BTC daily ladder | `bitcoin-above-on-{month}-{day}-{year}` event; keep today + next 2 days | 11 strikes × 2 × 3 ≈ 66 | A strike ladder: monotonic constraint, `Φ(d₂)` fair value, 0.001 tick (no tick floor), volume ramps from calm to toxic over 7 days. Resolves on Binance BTC/USDT 1-min close at 12:00 ET. |
| Fed Dec 25 bp hike | fixed slug `will-the-fed-increase-interest-rates-by-25-bps-after-the-december-2026-meeting-20260729232808636` (+ sibling outcomes of the neg-risk event) | 2 (8 with siblings) | Calm, rewards-heavy ($500/day), neg-risk group (Σp = 1), one scheduled jump 2026-12-09. The Phase 4 baseline. |

≈ 75 tokens, one WS connection. Rejected for batch 1: politics, midterm markets (resolve within days), Le Pen 2027 (0.001 tick, thin), sports (later batch).

### 2.7 Build plan

Build mode (decision 5): Claude builds steps 1–7 end to end with `# Why:` comments; Yehor does not edit code; checks and the audit are run together; `VERIFY` findings are confirmed by Yehor before they are baked into schemas.

| Step | Deliverable | You verify by |
|---|---|---|
| 0 | Decisions below answered | — |
| 1 | Repo skeleton: `pyproject.toml` (uv, py3.12; deps `websockets httpx pyarrow polars duckdb pydantic pyyaml`), `src/polymm/{config,records,sources,parsers,book,sinks,recorder,health}.py`, `scripts/`, `data/` git-ignored | `uv run python -c "import polymm"` |
| 2 | `UniverseWatcher` + Gamma client; `config.yaml` with the three family rules | `uv run polymm universe` prints the tokens that would be subscribed right now, with questions |
| 3 | `WsSource` + `RawSink` only (dump frames, zstd) | Run 10 min; decompress; count `event_type`s; confirm `VERIFY` items (delta semantics, trade side) |
| 4 | `Parser`, `BookState`, `ParquetSink`, `Recorder` wiring | Run 10 min; `duckdb` → `SELECT event_type, count(*) FROM 'data/events/book_events/**/*.parquet' GROUP BY 1`; plot best bid/ask for one token |
| 5 | `RestPoller`, reconnect, `Health` | Turn Wi-Fi off 30 s; see `reconnects=1, gaps≥1, snapshot_rest` rows; book matches REST after |
| 6 | `launchd` plist (laptop) with `caffeinate`; `scripts/audit.py` (msgs/hour, gaps, mismatches per token) | Run overnight; audit in the morning |
| 7 | `scripts/backfill.py` for D6, D8, D9 | Row counts vs Data API totals; re-run → no duplicates |
| 8 | VPS (Hetzner CX22, EU) + `systemd` + nightly `rsync` back | Compare one day laptop vs VPS: same msg counts ± reconnect gaps |
| 9 | 2026-12-01: diff one token's day vs Tardis free dataset | Same trades, same snapshot counts within tolerance |

Steps 1–5 in one go, then 6–7. Step 8 when 6 has run clean for 2–3 days.

---

## 3. Storage

### 3.1 Principles

- **Raw → events → derived.** Three layers. Raw is never modified. Events are append-only parsed tables. Derived (top-of-book series, markout tables, book reconstructions) is built later by research code and can always be rebuilt from events.
- **Parquet + Hive partitions, local now, same layout on S3 later.** Column store, ~10× compression on repetitive columns, predicate pushdown on partitions, zero-copy into polars/DuckDB/pandas. SQLite/Postgres would force row-at-a-time inserts and give nothing we need.
- **Query with DuckDB (SQL) or polars (Python).** Both read a partitioned directory as one table: `SELECT * FROM 'data/events/trades/**/*.parquet' WHERE dt='2026-11-06'`.
- **Timestamps are `int64` epoch UTC.** Nanoseconds for `ts_recv`, milliseconds for `ts_exch`. Never tz-naive datetimes, never strings.
- **Prices `float64`, sizes `float64`, ids `string`.** Token ids exceed int64; they stay strings.

### 3.2 Layout

```
POLY MM/
├── config.yaml
├── data/                                  # git-ignored
│   ├── raw/
│   │   └── ws_market/dt=2026-11-06/hour=14/frames-140001-003.jsonl.zst
│   ├── events/
│   │   ├── book_events/dt=2026-11-06/hour=14/part-141503-00012.parquet
│   │   ├── trades/     dt=2026-11-06/hour=14/part-….parquet
│   │   ├── tob/        dt=…/hour=…/part-….parquet      # server-side best bid/ask events
│   │   ├── tick_changes/, resolutions/
│   │   └── health/     dt=2026-11-06/hour=14/part-….parquet
│   ├── reference/
│   │   ├── markets/    dt=2026-11-06/hour=14/part-….parquet  # D1 (+D2) rows on subscribe/unsubscribe/refresh
│   │   ├── events/     dt=2026-11-06/events.parquet         # D10
│   │   ├── oi/         dt=2026-11-06/oi.parquet             # D11
│   │   └── resolutions/resolutions.parquet                  # D9, overwritten
│   ├── backfill/
│   │   ├── trades_hist/condition_id=…/part-0001.parquet     # D6
│   │   └── prices_hist/token_id=…/bucket=60/part-….parquet  # D8
│   └── derived/                                             # research output, later
└── src/polymm/…
```

Partitioning is `dt=/hour=`; `asset_id` is a column (≈80 tokens × 30 s flushes would otherwise mean thousands of tiny files per hour). Implemented schemas: `src/polymm/records.py`.

### 3.3 Schemas (events layer)

**`book_events`** — one row per level change or per level of a snapshot

| column | type | note |
|---|---|---|
| `ts_recv_ns` | int64 | local, before parse |
| `ts_exch_ms` | int64 | from message |
| `asset_id` | string | partition key |
| `condition_id` | string | |
| `event_type` | string | `delta` · `snapshot_ws` · `snapshot_rest` · `reconnect` |
| `snapshot_id` | int64 | groups rows of one snapshot; null for deltas |
| `side` | string | `BUY` (bid) / `SELL` (ask) |
| `price` | float64 | |
| `size` | float64 | new total at level |
| `hash` | string | |
| `best_bid` | float64 | from delta msg; null in snapshots |
| `best_ask` | float64 | |
| `conn_id` | int32 | which connection produced it |

**`trades`**

| column | type |
|---|---|
| `ts_recv_ns, ts_exch_ms` | int64 |
| `asset_id, condition_id` | string |
| `price, size` | float64 |
| `side` | string (taker side) |
| `fee_rate_bps` | int32 |
| `tx_hash` | string |
| `conn_id` | int32 |

**`tick_changes`**: `ts_recv_ns, ts_exch_ms, asset_id, old_tick, new_tick`.

**`health`**: `ts_ns, conn_id, msgs_total, msgs_book, msgs_delta, msgs_trade, reconnects, gaps, hash_mismatches, queue_depth, write_latency_ms, bytes_written`.

**`markets`** (reference): the D1 fields as columns, plus `rate_per_day` from D2, plus `fetched_at_ns`. One file per day; history of parameter changes falls out for free.

**`trades_hist`** (backfill): D6 fields as-is plus `fetched_at_ns`.

### 3.4 Sizes and retention (measured 2026-11-05, 82 tokens, BTC 15m dominating)

| | measured rate | per day | 30 days |
|---|---|---|---|
| WS messages | ~1,000 msgs/s, ~1,700 delta rows/s | ~90 M msgs | — |
| raw jsonl.zst (level 12, 9.4×) | 630 KB/s uncompressed | ~6 GB | ~180 GB |
| book_events + tob parquet (zstd) | ~55 KB/s | ~5 GB | ~150 GB |
| trades, health, reference | — | < 50 MB | — |

The BTC 15m market alone is ~480 deltas/s per side; it is bot-requoted continuously. The laptop has ~108 GB free, so this is **~10 days of headroom**, not months. Levers, in order: (1) every delta is mirrored exactly on the sibling token in the same frame (verified) — store one side per market and derive the other → halves `book_events` (**done 2026-11-07, see §5 A1**); (2) nightly offline recompress of raw at zstd-19 (`polymm compact`) and drop raw for validated days; (3) VPS with a bigger disk / object storage.

### 3.5 Tooling

| Need | Tool | Why |
|---|---|---|
| Write Parquet | `pyarrow` | Native, schema from dataclass, zstd |
| Raw compression | `zstandard` | Level 19: ~15–25× on repetitive JSON, fast decompress |
| Read / research | `polars` + `duckdb` | Lazy scans over partitions; SQL when it's clearer |
| Config | `pydantic` + yaml | Validated settings |
| Env / deps | `uv` | Fast, lockfile, reproducible |
| WS / HTTP | `websockets`, `httpx` | asyncio-native |
| Service | `launchd` (mac) → `systemd` (VPS) | Restart on crash, start on boot |

---

## 4. Decisions (approved 2026-11-05)

| # | Decision | Choice |
|---|---|---|
| 1 | Market batch | BTC 15m (rolling) + BTC daily strike ladder (today + 2 days) + Fed December 25 bp hike. No politics. See §2.6. |
| 2 | Storage | Parquet + Hive partitions, raw → events → derived, int64 epoch timestamps, DuckDB/polars. As §3. |
| 3 | Where it runs | Laptop first under `launchd`; VPS (Hetzner, EU) after 2–3 clean days. |
| 4 | Raw frames | Keep, zstd level 19, hourly files. |
| 5 | Build mode | Claude builds end to end; checks and audit run together; Yehor does not edit code. |
| 6 | Tooling | Python 3.12, `uv`, package `polymm`, deps `websockets httpx pyarrow polars duckdb pydantic pyyaml zstandard`. |

---

## 5. Verified on live data (2026-11-05) and known quirks

- **Delta `size` = new total at the level; 0 = level removed.** Replaying snapshot → deltas → next snapshot matched exactly in 242/242 clean windows.
- **Every delta is mirrored on the sibling token in the same frame** (YES bid p ≡ NO ask 1−p), 317,748/317,748 rows. Storing one side per market is lossless.
- **Frames can be JSON lists** of several messages; the parser handles both.
- **`book` snapshots arrive spontaneously** (~4/s across 82 tokens), not only on subscribe. Plenty of replay anchors even without the REST poller.
- **Subscribe/unsubscribe ops on an open connection work**; `price_change` for a market carries both of its tokens even if only one was subscribed.
- **Server-side `best_bid/best_ask` on deltas disagree with the reconstructed book in ~0.4% of frames**, while the reconstructed book is exact against snapshots. Treat the reconstructed book as truth; the fields are stored for reference.
- **"Slow consumer" disconnects** happen if the WS reader is starved for ~1 s. Fixed by yielding in the consumer every 20 messages and `max_queue=4096` on the websockets client.
- **Volume** is far above the first estimate (see §3.4); storage decisions needed within ~10 days.
- **A1, one token per market in `book_events` (live from 2026-11-07).** Outcome 0 (`outcome_index` in `markets`; Yes / Up) is stored in full. Outcome 1 rows are stored only where they are not redundant:
  - a `price_change` message's outcome-1 deltas are dropped iff they are exactly the mirror (side flipped, price `1 − p`, same size) of its outcome-0 deltas;
  - an outcome-1 snapshot is dropped iff it equals both the token's current book and mirror(outcome 0), i.e. it changes nothing. (Matching the mirror alone is not enough: the server's NO book briefly differs from mirror(YES) at ~0.03% of checks, and a snapshot that resyncs it is a real change.)
  - **Rebuild rule for research:** outcome 1 = replay of its own stored snapshots + per delta message its own stored rows if any, else mirror(outcome-0 rows of that message). Use `book.flip_price` / `book.mirror` (floats: `1 − 0.57 ≠ 0.43` without rounding).
  - Verified on 1 h of raw frames (2026-11-06 15:00, 3.58 M frames): 49.9% fewer rows (deltas −50.0%, ws snapshots −48.9%); books rebuilt from stored rows matched the full books in 6,201,126 / 6,201,126 checks. Health columns `mirror_dropped` / `mirror_kept` watch it live; the audit prints them.
  - Schema rule that came with it: columns are only ever added at the end; readers use `union_by_name` (see `records.py`).
- Still open: Austria geo-restriction (Phase 6); whether `fee_rate_bps` on trades is per-order or per-market.

---

## 6. Feeds for the fair-value research (proposed 2026-11-13, not yet recording)

> Status: **proposal**. Nothing in this section records yet except the rows marked *recorded*.
> Since 2026-11-13 only BTC 15m and Binance spot are recorded (D-041). Mind-map node IDs refer
> to Notion "Fair-value prediction — breakdown".

**How the sizes were measured.** Every feed was sampled live for 600 s from 2026-11-13 18:37 UTC
(`ops/feed_sample.py`, results `reports/feeds/sample_2026-11-13T1837Z_600s.json`). Each was turned
into GB/month with `ops/feed_sizes.py`, calibrated on what we really store for Binance spot:
- On 2026-11-12 we stored 16.64 M spot messages as 162 MB raw + 242 MB Parquet = 404 MB/day.
- Activity factor 1.29: that day's average message rate ÷ the sample's rate.
- Storage factor 5.0: stored bytes ÷ zstd-12 bytes of the bare JSON, for raw + Parquet together.

The same two factors are applied to every feed. So each "GB/month" means raw archive + parsed
Parquet at our current formats, on an average day. A volatile month can be 2–3× higher.

All feeds below are **free and public, with no key**, except where noted.

| Tier | Feed (source → stream) | API | Data points | For (mind-map node) | msg/s | GB/month |
|---|---|---|---|---|---|---|
| *recorded* | Polymarket CLOB market WS, BTC 15m only | WS | book deltas, snapshots, trades, top of book, tick changes, resolutions | A2, B, C2: the target itself | ~450 | ~85 (est. after D-041: raw ~55, Parquet ~30) |
| *recorded* | Binance spot `bookTicker` + `trade` | WS `data-stream.binance.vision` | best bid/ask on every change, every trade | A1 discovery venue | 150 | 12 |
| *recorded* | Gamma / CLOB REST | REST | markets, strikes (`priceToBeat`/`finalPrice`), rewards, REST book anchors | reference | – | < 1 |
| 1 | Polymarket RTDS `crypto_prices_chainlink` btc/usd (legacy; successor PolyBolt `price.crypto.twap` needs CLOB API credentials) | WS `ws-live-data.polymarket.com` | Chainlink BTC/USD, 1 per second | A1 settlement source: the gap $b$ is estimated today | 1.0 | 0.4 |
| 1 | Polymarket RTDS `crypto_prices` btcusdt | WS, same | Polymarket's own Binance reference | A1, what other bots see | 1.0 | 0.2 |
| 1 | Binance USDⓈ-M perp `bookTicker` | WS `fstream…/public/` | best bid/ask on every change | A1, C1: perp usually leads spot | 629 | **75** |
| 1 | Binance perp `aggTrade`, `markPrice@1s`, `forceOrder` | WS `fstream…/market/` | trades with aggressor; mark, index, funding; liquidations (sampled: ≤ 1/s per symbol) | C1 aggressor flow, liquidations; funding as regime only | 11 | 2.5 |
| 1 | Binance perp + spot `depth20@100ms` | WS | 20 levels each side every 100 ms | C1 imbalance, multi-level OFI | 20 | 11 |
| 1 | Coinbase Exchange `ticker` + `matches` BTC-USD | WS `ws-feed.exchange.coinbase.com` | top of book with each trade, every trade | A1 USD venue (no USDT), US-hours discovery | 24 | 14 |
| 1 | OKX `bbo-tbt` + `trades` BTC-USDT-SWAP; `liquidation-orders`, `funding-rate`, spot `bbo-tbt` | WS `ws.okx.com/ws/v5/public` (port 443; 8443 is being retired) | best bid/ask every 10 ms on change, trades, liquidations | A1 venue fusion, C1 | 48 | 9.4 |
| 1 | Bybit linear `orderbook.1` + `publicTrade`; `allLiquidation`, `tickers` | WS `stream.bybit.com/v5/public/linear` | top of book, trades, **all** liquidations, OI, funding | A1, C1 liquidations | 34 | 15 |
| 1 | Deribit `deribit_volatility_index.btc_usd`, `markprice.options.btc_usd`, `ticker.BTC-PERPETUAL.100ms` | WS `www.deribit.com/ws/api/v2` | DVOL, every option's mark IV, perp ticker | A1 σ (largest oracle gain in fair v1, §5 of `research/fair_value.md`) | 4.8 | 7.7 |
| 2 | Binance perp `depth@100ms` (full diff book) | WS | every level change, 100 ms batches | C1 deep-book OFI, if depth20 shows signal | 10 | 71 |
| 2 | Binance spot `depth@100ms` | WS | ″ | ″ | 10 | 16 |
| 2 | Coinbase `level2_batch` | WS (Exchange; `level2`/`full` now need auth) | level changes, 50 ms batches | A1 depth on the USD venue | 16 | 27 |
| 2 | Bybit linear `orderbook.50`, spot `orderbook.1` + trades; OKX `books5` | WS | 50 / 5 levels | C1 | 63 | 29 |
| 2 | Polymarket RTDS `activity` (`orders_matched`, all markets; keep BTC 15m rows) | WS, same as RTDS | trades **with `proxyWallet`** | A2.4 wallets / who is toxic | 34 | 31 unfiltered, ~2–5 filtered |
| later | Binance data.binance.vision futures `aggTrades`, `bookDepth`, `metrics` | free daily files | history for backtests before our start | backfill | – | – |
| paid | Tardis.dev (historical tick books, all venues); Chainlink Data Streams direct | REST / files | – | not needed now | – | – |

**Totals per month.**
- Recorded now: ~100 GB.
- Plus tier 1: ~240 GB, about 8 GB/day.
- Plus tier 2: ~420 GB, about 14 GB/day.
- A year at tier 1 is about 3 TB.

**Clock and location.** The nowcast is about what *we* can know at time $t$, so every row keeps
our receive stamp next to the venue's own timestamp. The two give each feed's staleness.
Lead–lag measured on our receive clock includes each venue's network path to the recorder:
- Binance, OKX and Bybit run in Tokyo.
- Coinbase runs in the US East.
- Deribit runs in London.

Those numbers are only valid for the machine that will trade, so the recorder belongs where the
engine will run (measure as in D-031).

**Capacity on the current VPS** (2 vCPU Arm, 4 GB, 38 GB disk, keeps 7 days):
- CPU and memory are fine. The recorder uses ~11 % of one core and 0.3 GB at ~600 msg/s. Tier 1
  adds ~800 msg/s, mostly perp `bookTicker`.
- The **disk is not**: 7 days at tier 1 is ~55 GB against 33 GB free. Hourly uploads to the
  bucket (ARCHITECTURE step 2) therefore come before, or together with, tier 1.
- New feeds run as a second service on the same box. It is the same system clock (chrony), and
  a fault in it cannot stop the Polymarket recorder.
