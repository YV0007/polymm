# Decisions log

The single record of *what we decided and why*. Plans and daily status live in Notion
(see CLAUDE.md §0); data lives in `data/` and on the VPS. Nothing here is duplicated there.

**Rules**
- **Append-only.** New entries go at the bottom with the next number. Never rewrite an old entry.
- **Changing a decision** = a new entry that says `Supersedes D-xxx`; the old entry's status line
  is the only thing you may edit (`superseded by D-yyy`).
- **One decision per entry**, short. Link the commit / doc section that implements it.
- Any chat (Claude session) that makes a decision adds it here in the same commit as the code.
- On a separate worktree/branch, read the main copy: `/Users/macbook/Desktop/POLY MM/docs/DECISIONS.md`.

Template:

```
### D-NNN · YYYY-MM-DD · Title
- **Decision:** …
- **Why:** …
- **Rejected:** … (only if alternatives were actually considered)
- **Ref:** commit / file § / Notion page
- **Status:** active | superseded by D-NNN
```

---

### D-001 · 2026-11-05 · First market batch
- **Decision:** BTC 15m up/down (rolling) + BTC daily strike ladder (today + 2 days) + Fed December 2026 decision event. No politics.
- **Why:** BTC 15m gives many windows quickly (~96/day) for markout statistics; the ladder adds strike structure; Fed adds a rewarded, neg-risk, large-ticket market.
- **Rejected:** 4-category mix (politics/macro, sports, short crypto, rewarded).
- **Ref:** DATA_PLAN §2.6, §4 #1; `config.yaml` families
- **Status:** active

### D-002 · 2026-11-05 · Storage format
- **Decision:** Parquet + Hive partitions, layers raw → events → derived, int64 epoch timestamps, queried with DuckDB/polars.
- **Ref:** DATA_PLAN §3, §4 #2
- **Status:** active

### D-003 · 2026-11-05 · Where the recorder runs
- **Decision:** Laptop first under `launchd`, then a Hetzner EU VPS after 2–3 clean days.
- **Ref:** DATA_PLAN §4 #3
- **Status:** superseded by D-012 (VPS cut-over done)

### D-004 · 2026-11-05 · Keep raw WebSocket frames
- **Decision:** Store every raw frame verbatim (zstd, hourly files) before parsing.
- **Why:** Parser bugs or schema misunderstandings become a re-run, not lost data.
- **Ref:** DATA_PLAN §2.4 #4, §4 #4
- **Status:** active (compression level changed by D-009)

### D-005 · 2026-11-05 · Build mode
- **Decision:** Claude builds the code end to end, with `# Why:` comments and an explanation of key decisions; checks and audit run together with each step; Yehor runs and reviews, does not edit code.
- **Rejected:** Yehor writes plan + skeleton, Claude reviews (the original CLAUDE.md style).
- **Ref:** DATA_PLAN §4 #5; reconfirmed by Yehor in chat 2026-11-07
- **Status:** active

### D-006 · 2026-11-05 · Tooling
- **Decision:** Python 3.12, `uv`, package `polymm`; deps websockets, httpx, pyarrow, polars, duckdb, pydantic, pyyaml, zstandard. Rust only after an edge is proven.
- **Ref:** DATA_PLAN §4 #6; `pyproject.toml`
- **Status:** active

### D-007 · 2026-11-05 · Record our own books; third-party data only as validator
- **Decision:** The order book comes from our recorder. Tardis' free day (2026-12-01) is used only to validate the recorder.
- **Why:** The official API has no historical book snapshots; paid vendors don't remove the need for our own receive timestamps.
- **Ref:** DATA_PLAN §1.4
- **Status:** active

### D-008 · 2026-11-05 · Gap detection by hash comparison; reconstructed book is truth
- **Decision:** No sequence numbers exist, so every 60 s the REST `/books` hash is compared with the last WS hash per token. The locally reconstructed book is treated as truth; server `best_bid/best_ask` fields are stored for reference only.
- **Why:** Polymarket's hash algorithm is undocumented, so comparing their two hashes needs nothing; server best fields disagree with the (exact) reconstructed book in ~0.4% of frames.
- **Ref:** DATA_PLAN §2.4 #6, §5
- **Status:** active (refined by D-011)

### D-009 · 2026-11-05 · Raw compression zstd level 12 live
- **Decision:** zstd level 12 when recording; optional nightly recompress later.
- **Why:** Measured 9.4× at ~50 MB/s at level 12 vs 11.8× at ~2 MB/s at level 19: too slow for live.
- **Ref:** `config.yaml` `raw_zstd_level`
- **Status:** active (partly supersedes D-004's level 19)

### D-010 · 2026-11-06 · No large CPU step on the event loop
- **Decision:** Row → Arrow conversion runs in 15k-row slices with awaits between them; compression and file I/O run in a worker thread.
- **Why:** Converting 100k+ rows in one call froze the loop for 0.4–12 s and caused "slow consumer" disconnects.
- **Ref:** commit fb87f4d
- **Status:** active

### D-011 · 2026-11-07 · Confirm a gap before resetting a book
- **Decision:** A REST/WS hash mismatch is re-checked after 2 s against a fresh REST read before the book is reset; races are counted separately from gaps. `custom_feature_enabled` is sent on every subscribe op.
- **Why:** Most mismatches were queue/wire races, not real gaps. Without the flag, the server turns off `best_bid_ask` for the connection at every 15m roll.
- **Ref:** commit e1fcc4b
- **Status:** active

### D-012 · 2026-11-07 · Recorder on Hetzner VPS
- **Decision:** Recorder runs 24/7 on a Hetzner CAX11 (Arm, Falkenstein, 100 GB volume, ~€14.65/mo) since 09:35 UTC; laptop overlaps for one day, then is switched off.
- **Supersedes:** D-003
- **Ref:** commits e1fcc4b, 3719c6c; `ops/`; Notion project page (ops row)
- **Status:** active

### D-013 · 2026-11-07 · Store one token per market (A1)
- **Decision:** Outcome 0 (Yes/Up) stored in full; outcome-1 rows stored only where they are not an exact mirror of outcome 0. Research rebuilds outcome 1 by the rule in DATA_PLAN §5.
- **Why:** −49.9% `book_events` rows; rebuilt books matched full books in 6,201,126 / 6,201,126 checks.
- **Rejected:** storing both tokens in full.
- **Ref:** commit 52074de; DATA_PLAN §5
- **Status:** active

### D-014 · 2026-11-07 · Schema evolution rule
- **Decision:** Columns are only ever added at the end of a table; readers use `union_by_name`.
- **Why:** Old and new Parquet files stay readable together without migrations.
- **Ref:** commit 52074de; `records.py`
- **Status:** active

### D-015 · 2026-11-07 · Binance BTC/USDT spot feed
- **Decision:** Record Binance BTC/USDT bookTicker + trades with our own receive timestamps (own queue and raw sink); backfill daily history from data.binance.vision with checksum verification.
- **Why:** The BTC fair-value model needs spot, and lead–lag analysis needs our receive timestamps, which can't be recovered later.
- **Ref:** commit 4179aa9; Notion Global plan "Gap to close now"
- **Status:** active

### D-016 · 2026-11-07 · Nightly pull and verified prune
- **Decision:** Every night at 00:00 Vienna, rsync data from the VPS to the laptop; delete server day partitions older than 7 days only when the local copy matches file-for-file and size-for-size; never prune today/yesterday. Dead-man warning if yesterday has no `book_events`.
- **Ref:** commit 0fab07c; `ops/nightly_pull.sh`
- **Status:** active

### D-017 · 2026-11-07 · Research plan with two gates
- **Decision:** Work follows the 8-stage Global plan (infrastructure → replay & data quality → shape → fair value → adverse selection → Gate A → queue model → rewards → simulator → Gate B → live pilot).
- **Why:** Each stage builds the tool the next needs; gates stop us spending effort or money without evidence of edge.
- **Ref:** Notion "Global plan"
- **Status:** active

### D-018 · 2026-11-07 · Where project knowledge lives
- **Decision:** Decisions → this file (repo). Plans, daily steps, status, ops/cost → Notion. Data → `data/` and the VPS, never in either. Each fact lives in exactly one place; the other links to it.
- **Why:** Every chat reads the repo instantly and git ties each decision to its code; Notion is the human dashboard (phone-friendly, same across branches). One place per fact prevents drift.
- **Rejected:** everything in Notion (slower for Claude, drifts from code); everything in repo (plans change daily and are awkward to read on the phone).
- **Ref:** CLAUDE.md §0
- **Status:** active

### D-019 · 2026-11-07 · REST anchor every 5 min
- **Decision:** `rest_anchor_interval_s` 600 → 300.
- **Why:** replay trusts a window only up to its last verified snapshot; with 10-min anchors a healthy day lost ~7 % of time (loose 99.9 % → strict 92.8 %, `reports/2026-11-06/quality.md`). Anchors are a few hundred extra Parquet rows per hour.
- **Ref:** `config.yaml`; quality report §4 item 9
- **Status:** active

### D-020 · 2026-11-07 · Fill labels: trade → delta, one-to-one, size-aware
- **Decision:** In `events_labeled` each trade picks at most one size decrease per token (own token, and the mirrored level on the sibling): the nearest in time within ±1 s at its price on the side it consumed whose decrease is at least the trade size, else the nearest. Several trades may pick the same delta; `n_trades` and `trade_size_sum` (appended columns) say how many and how much. `label = fill` only on picked deltas.
- **Why:** The previous delta → nearest-trade rule gave every decrease near a trade the trade's label: 11.4 fill rows per trade on btc_15m, and one taker order hitting several makers at one level (several trade messages, ONE delta) left 12 % of trades looking unmatched.
- **Rejected:** keeping delta → trade and deduplicating afterwards (loses the multi-maker case).
- **Ref:** `replay.py` `_label_events`; `research/quality_report.md` §4 #2; `research/replay_report.md` §5
- **Status:** active

### D-021 · 2026-11-07 · Clean windows end at the token's last event; idle tokens get no reconnect rows
- **Decision:** A token's last window ends at its last non-reconnect event, not at the end of the replayed range. At a reconnect, a token with no event for more than `grid_max_idle_s` (15 min) is treated as unsubscribed: no `reconnect` row in `tob`, window closed at its last event with `end_reason = idle`.
- **Why:** BTC 15-minute tokens are unsubscribed ~10 min after their window ends; the old rule gave them windows until midnight and a `reconnect` row at every later reconnect, so time-weighted statistics counted hours of a book nobody recorded. A subscribed token gets a REST anchor at least every 5–10 min, so 15 min of silence means unsubscribed.
- **Rejected:** an explicit unsubscribe marker from the recorder (would work too, but only for new data).
- **Ref:** `replay.py` `_reconnect`, `Replayer.run`; `research/quality_report.md` §4 #1
- **Status:** active

### D-022 · 2026-11-08 · One WS connection per family, scoped reconnect markers, loop-lag in health, no auto-restart on upgrades
- **Decision:** (1) The recorder opens one Polymarket market-channel connection per family (`recorder.ws_shard_by_family`), conn_ids unique across them. (2) A break writes one `reconnect` row per market of that connection (`condition_id` set); `condition_id ""` keeps meaning "every book". Replay applies each row only to its market; quality/audit count distinct instants. (3) `health.loop_lag_max_ms` = worst event-loop freeze per 10 s. (4) needrestart never restarts `polymm.service` after library upgrades; restarts happen through `ops/deploy.sh`.
- **Why:** 11-07/08: 9 of 12 disconnects were Polymarket closing us with 1013 "slow consumer: send buffer full". It was not our side: in the 60 s before each cut Binance stamps on the same loop never paused > 1 s, Polymarket frames never > 1.6 s, our socket Recv-Q was ~0 and the 20 s before a cut carried only 1–2.5× the normal byte rate. The buffer that fills is theirs, per connection (via CloudFront). Less flow per connection fills it less often and a cut stays inside one family. Scoped markers keep the other families' clean windows open (a global marker would leave them stale until the next REST anchor, up to 5 min). The 06:30 UTC restart on 11-08 was unattended-upgrades → needrestart after libc/openssl updates.
- **Rejected:** a bigger client buffer (ours is not the one filling); duplicate redundant connections with arbitration (doubles storage, more complex; revisit for live trading); turning off custom features (`new_market` is ≤ 11 % of bytes and `market_resolved` is needed).
- **Ref:** `recorder.py`, `sources.py`, `parsers.py` `reconnect`, `replay.py` `Reconnects`, `ops/vps_setup.sh`
- **Status:** active

### D-023 · 2026-11-08 · Fair value v1: book level + anchored BTC model, weight fitted walk-forward
- **Decision:** `polymm.fair` defines the fair value of every YES token as $x^{\text{fair}} = x^{\text{book}} + w\,(x^{\text{anch}} - x^{\text{book}})$ in log-odds. Book = Stoikov's empirical microprice (mid + mean next-5 s mid move per imbalance × spread bucket). Model = BTC digital $\Phi(d)$ with the strike from Gamma `priceToBeat`, the Binance–Chainlink gap measured at window start, $\tau_{\text{eff}}$ for the 60 s settlement average and the step-5 $\sigma$. Anchor = model + EWMA of (book − model), half-life chosen on training days. $w = \sigma(\beta \cdot f)$ in minute/time-to-expiry and $|d|$, fitted at τ = 30 s on earlier days only. Ladder made monotone in strike; Fed = microprices rescaled to sum to 1. Scoring: skill vs the future mid at 1/5/30/60 s, slope, hit rate, Brier, SEs clustered by window. Also: step-5 implied σ now uses $\tau_{\text{eff}}$.
- **Why:** held-out 11-07/08: the raw model's level is a median 0.35 log-odds off the market (Brier 0.161 vs mid 0.1525), but its changes lead the mid (72 % same-direction when both move). Anchored and blended it beats mid and Stoikov at 30–60 s in both BTC families (btc_15m +0.016 vs +0.009; minutes 10–12 +0.033) with the best Brier (0.1521). Oracles rank future work: σ forecast > earlier BTC signal > Chainlink gap > our 128 ms latency (worth nothing).
- **Rejected:** blending the model's level directly (the plan's first form; worse than the mid everywhere); "book + model move since the book last changed" (model micro-moves are noise the 1-cent book never follows; skill fell at every horizon); the raw microprice as the book (its level offset inside the spread costs it at ≤ 30 s).
- **Ref:** `src/polymm/fair/`, `research/fair_value.md`, `reports/fair/summary.md`, `reports/fair/fair_params.json`
- **Status:** active (provisional: two held-out days)
