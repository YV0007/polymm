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

### D-024 · 2026-11-08 · Adverse selection v1: public trades as maker fills, YES-equivalent, exact-time references, Gate A by select-then-confirm at τ* and 60 s
- **Decision:** `polymm.markouts` turns every public trade into the maker's fill on the YES token ($s = +1$ maker bought YES; a NO-token print at $q$ is YES at $1-q$), reads the book just before the taker order's earliest consumed book decrease (fallback 50 ms before the trade), and measures $h$, $M(	au)$, $a(	au)$ for τ = 1 s … 1 h and to resolution against the exact mid and the exact mid plus the **walk-forward** D-023 fair premium; horizons stop at the clean-window end or the market close, whichever is first. SEs clustered per 15m window / market-hour. τ* = first horizon whose remaining decay to 60 s is not significant, fixed on the training day. $\hat M(\tau \mid z)$ = ridge on ex-ante features only (taker size excluded), walk-forward. Gate A: a slice counts only if selected on training days and positive at 95 % on every held-out day at τ* AND at 60 s; a family whose τ* is a fallback is INCONCLUSIVE.
- **Why:** 11-06…08, 233k fills: btc_15m makers capture 0.83 ¢ and give it all back within 1 s (τ* = 1 s; size-weighted −0.29 ± 0.14 ¢ at 1 s); the 0.26 ¢ rebate is what keeps the average maker at zero. Ex-ante features separate the flow out of sample (drop the worst half → +1.22 ¢ at 1 s, +1.55 ¢ at 60 s). Gate A **GO (provisional) for btc_15m on spread > 5 ¢** (+2.12/+3.41 ¢ at 1 s, +2.70/+3.60 ¢ at 60 s on 11-07/08); queue imbalance and the model-kept set pass at 1 s but not at 60 s on 11-07; ladder and Fed inconclusive. The final-fit `fair.parquet` would have overstated the 60 s gap scorer (+2.13 vs +0.82 ¢ after dropping half).
- **Rejected:** the raw token's `side` (94 % BUY); the book at the trade's receive time (post-trade in 21 % of cases); per-message pre-trade time (2nd sweep level saw a half-swept book); the 1 s fair grid value at the fill (up to 1 s stale, +0.2 ¢ spurious h); "80 % of decay to 300 s" and "last significant step" for τ*; one confirming day, or a 60 s point estimate, for a slice; `fee_rate_bps` from trades (always 0). Items 3–6 came from an independent code audit before commit (no sign error or lookahead found).
- **Ref:** `src/polymm/markouts/`, `research/adverse_selection.md`, `reports/markouts/summary.md`, `reports/markouts/as_params.json`, `tests/test_markouts.py`
- **Status:** active (provisional: two held-out days; average maker, not our queue position — stage 5)

### D-025 · 2026-11-09 · One research dashboard, refreshed every morning
- **Decision:** All review numbers live on one claude.ai Dashboard artifact (https://claude.ai/artifact/Lp6FVttHUNMFGWziGZ5uBk): data collected, quality, fair value (D-023), markouts and the expectation model (D-024), Gate A, every automated check and the test suite, plus a history row per day. `polymm dashboard` gathers them into one JSON file (`reports/dashboard/dashboard_data.json`); each section of the page is a dataset that reads its key from that file. `ops/daily_refresh.sh` pulls the server data, reruns quality + vol for each day without a `.complete` marker, then fair, markouts and the dashboard file; the scheduled task `polymm-dashboard-refresh` (07:30 Vienna) runs it, uploads the file and repoints the datasets.
- **Why:** reading tables across `reports/`, `research/` and Notion was too slow for review. One file per refresh keeps the update a single upload; the marker lets a day first run on partial data (11-08 at 15:40) be redone in full the next morning. Plots stay off in the daily run so tracked PNGs in `research/` are only rewritten on purpose.
- **Also:** Φ in `fair.models` now runs as a numpy loop, not a polars UDF: parallel UDFs raced on polars' lazy numpy import and killed a markouts run (2026-11-09).
- **Ref:** `src/polymm/dashboard.py`, `ops/daily_refresh.sh`, `~/.claude/scheduled-tasks/polymm-dashboard-refresh/`
- **Status:** active

### D-026 · 2026-11-09 · Queue and fill model: virtual orders on the replayed book
- **Decision:** `polymm.queue` drops virtual 5-share orders ("probes") into the replayed L2 book and tracks the shares ahead of each one per level update: a taker's consumption (the D-020-linked trade volume, applied at the book decrease) eats the queue from the front, adds join behind, cancels move us by one of four cancel models (pessimistic 0 / proportional x/L / matched = pseudo-L3 exact-size LIFO match of a cancel to an earlier add, else proportional / optimistic), x ≤ L after every update, and a trade at a worse price on our side fills us. Trades are timed at their taker order's start (earliest linked decrease, stage 4's rule). BTC 15m: probes every 2 s per side in the live window (touch, one tick behind, price-improving), a 0/50/300/1000 ms latency sweep every 10 s, and every spread opening; ladder and Fed every 10 s, descriptive. Stops: 60 s, window/market end, left behind (2 ticks), post-only rejection. Three policies: passive, `alone` (cancel when our side moved through our price) and `stale` (alone, or the fair value crossed our price), cancel latency 128 ms (swept 0–1000 ms), applied exactly after the fact. Probe fills are scored with the stage-4 markout engine; value of a placement V = 1{filled}·(M(τ) + rebate).
- **Why:** stage 4 measured the average resting maker; a newcomer at the back of the queue, 128 ms late, gets different fills. 11-06…08, 1.9 M probes: on BTC 15m every placement loses (touch, 60 s, incl. rebate: passive −1.16 ¢, stale −0.29 ± 0.03 ¢, instant cancel −0.15 ¢); our fills' a(1 s) is 1.3–2.0 ¢ against the average maker's 0.8 ¢ and rises with queue depth from 0.6 ¢ (alone at a new best) to 2.4 ¢ (1k+ ahead). The stage-4 Gate A slice (> 5 ¢ spread) fails for our own orders under every policy and cancel model. Conclusions hold across the four cancel models. Q0: continuity breaks < 0.02 %, 98–99 % of traded volume linked, level size = top of book 99.9 %, pseudo-L3 matches 92–95 % of cancels.
- **Rejected:** probes before the BTC 15m window (70 % of the first grid; flat, no fair value; diluted every number); trade-message timing (phantom fills from sweeps already under way, cancels beating the sweep's own fill); x0 (queue on arrival) as a decision feature or as "front of the queue" (128 ms lookahead; x0 = 0 at the touch meant swept in flight); one "realistic" cancel model instead of the bracket. The stale/alone policies were added after the first (passive) run, which showed 58 % of fills came from the price trading through the order: stated in the report, not pre-registered. Items 1–3 came from an independent code audit before commit.
- **Ref:** `src/polymm/queue/`, `research/queue_fill.md`, `reports/queue/summary.md`, `reports/queue/queue_params.json`, `tests/test_queue.py`; dashboard section `queue_*` (D-025); `ops/daily_refresh.sh` runs `polymm queue --reuse` (content fingerprint per day).
- **Status:** active. Implication: spread capture on BTC 15m needs a faster stale signal and cancel (Binance-driven, sub-second) and price-improving placement only, plus rewards (stage 6) — test the sub-second cancel trigger in this engine before the simulator.

### D-027 · 2026-11-10 · Pipeline first: v1 of every stage to a P&L number, then deepen by attribution
- **Decision:** Build stages 6 (rewards & fees) and 7 (simulator) as crude v1s next, so the whole chain fair value → adverse selection → queue/fills → rewards → simulator produces an expected P&L with the ①–④ decomposition. Only then deepen the parts the attribution says matter (likely candidates: tick-level Binance fair value and lead–lag, measured end-to-end latency, the cancel/reposition trigger, the selection model, more data days). Gates A/B are read as **provisional on v1 models**: a v1 FAIL means "a naive quoter loses", not "the market has no edge". The Binance repositioning test moves from "next" into the deepening phase.
- **Why:** every component is a one-day v1 (1 s fair-value grid, no measured latency, grid placements, no selection, 3 days of data); stage 5's "every placement loses" is a floor for a blind 128 ms quoter, not a verdict. Optimising one part before the chain exists risks polishing the wrong part; the simulator's attribution tells us which term (② adverse selection, fills, ④ rewards) moves P&L most.
- **Rejected:** deepening BTC 15m spread capture now (Binance cancel trigger, selective quoter) before rewards and the simulator exist; declaring BTC 15m unprofitable from v1 results.
- **Ref:** D-017 (Global plan), D-024 (Gate A), D-026 (stage 5 floor), `research/queue_fill.md`
- **Status:** active

### D-028 · 2026-11-10 · Simulator and pilot validation: walk-forward, stability, paper trading, sim-vs-live comparison
- **Decision:** Stage 7 and the stage 8 pilot follow the desk validation loop: sim → paper → tiny live → compare predicted with actual fills → recalibrate. (1) **Stage 7:** the ①–④ decomposition is computed for every rung of the strategy ladder (symmetric → A–S/GLFT skew → pull-on-BTC-move → reward-optimised), starting with symmetric as the baseline, so each rung's change shows up as a change in specific terms. Parameters are fitted walk-forward (fit on days 1…k, test on day k+1, roll). Every headline number is reported with its pessimistic…optimistic fill band and a parameter-stability check (P&L as γ, k, latency and cancel thresholds vary); the number of variants tried is logged so the best Sharpe can be deflated (Bailey–López de Prado). (2) **Gate B** also requires out-of-sample (walk-forward) P&L > 0 under pessimistic fills, and a smooth parameter surface, not a single peak. (3) **Stage 8** starts with **paper trading**: the simulator runs forward on the live feed on the VPS with virtual orders, with no account or money involved. Tiny live size follows, and its first output is the **sim-vs-live comparison**: predicted vs actual fill rate, queue position at fill and markout per fill, plus the live/sim P&L ratio, used to discount the simulator and recalibrate the queue model (D-026).
- **Why:** a replay simulator is a hypothesis about fills; only our own live fills test it. We have no L3 (queue position is inferred, D-026), so the gap between simulated and live fills is likely larger than on an exchange that publishes L3. Tuning a strategy inside the simulator otherwise optimises the fill model's error. Paper trading cannot look ahead, so it catches lookahead bugs (exchange vs receive timestamps) that a replay can hide. Diagnostics: a simulated MM Sharpe of 10+ points to a fill or lookahead bug, not an edge.
- **Rejected:** treating a single simulated P&L number as the Gate B input; going from the simulator straight to live size without paper trading.
- **Ref:** D-017 (Global plan), D-026 (queue model), D-027 (v1 results are floors); Notion Global plan, stages 7, B, 8
- **Status:** active

### D-030 · 2026-11-10 · One data store, a pull that cannot leave broken files, honest gap counter; no 15-minute connection split
- **Decision:** (1) `data_vps/` is the single store of record: the laptop recording is kept only before the server's first instant, 2026-11-07 09:35:45.919 UTC, merged in by `ops/merge_laptop_store.py` with one global `reconnect` row at the cut; `data` is a symlink to it; the laptop's later copy (4.6 GB) went to the Trash; the laptop recorder is stopped and disabled in launchd. (2) `ops/nightly_pull.sh` uses `--partial-dir` (an interrupted file never appears under its real name), `--timeout=120`, ssh keepalives and 3 attempts; it prunes a server day when every server file is present locally with the same size (the local store may hold more). (3) `ops/daily_refresh.sh` stops on a failed pull, on any damaged Parquet file (`ops/check_parquet.py`, PAR1 magic at both ends) or on a failed day, before fair/markouts/queue. (4) The recorder's `gaps` counts only confirmed gaps where the book was reset; REST answers older than our book go to a new health column `rest_stale`. (5) The server was rebooted onto the pending kernel (37 s without data, 11:18:35–11:19:12 UTC).
- **Why:** 11-10 08:05 UTC the server stopped answering mid-pull; `--partial` left a 64 KB stub of a Parquet file under its real name and the whole morning refresh failed on it (the scheduled task correctly refused to upload). Laptop vs server over the overlap (11-07 09:35 → 11-10 11:08): the server holds 2–6× more book changes and trades (11-09: 43.8 M vs 16.3 M changes, 63 k vs 26 k trades); laptop-only rows are 0.02–0.06 % of the server's per day after 11-07 and sit in the server's 1 s connection cuts; on 11-07, 87 % of the laptop-only rows are 09:00–09:35 (before the server) and outcome-1 rows the newer code drops by design (A1). Matched trades reach the server a median 3 ms earlier. Of 28 "gaps" on 11-08, 24 were REST lagging by ~4 ms.
- **Rejected:** splitting the btc_15m connection further. It drops about once an hour on weekdays (40 of 50 cuts were Polymarket's "slow consumer"; our loop never froze > 116 ms before one), but 97 % of its bytes are the live window's own market (233 kB/s average, 788 kB peak second), and subscribing to one token still delivers both tokens' changes (5.27 MB vs 5.31 MB in 25 s), so no split on our side lightens it. Each cut costs about 1 s of that market and replay marks it. The real remedy, a duplicate connection with de-duplication (A/B feed arbitration), waits for live trading. Also rejected: patching the server's 1 s cuts with laptop rows (two recorders' books spliced mid-stream).
- **Ref:** `ops/nightly_pull.sh`, `ops/daily_refresh.sh`, `ops/check_parquet.py`, `ops/merge_laptop_store.py`, `src/polymm/poller.py`, `records.py` (`rest_stale`)
- **Status:** active

### D-029 · 2026-11-10 · Rewards and fees v1: the published rules, our share bracketed, cost from the probes, a screen over every paying market
- **Decision:** `polymm.rewards` implements the liquidity-reward rules as published (docs read 2026-11-10: S = ((v − s)/v)², Q_one/Q_two on the YES book with NO orders mirrored, Q_min two-sided rule with c = 3 inside 0.10–0.90, per-minute random sampling, daily normalisation) and the fee/rebate rules (taker fee C·f·p(1−p), rebate exactly r·f·p(1−p) per filled share). Configuration from `/rewards/markets/current` + `/sampling-markets` (snapshot per day under data/reference/rewards/); fees on/off from each market's `taker_base_fee`, the rate from its sector (tags mapped onto the fee categories). On our recorded pools: one random second per minute in strict windows, the book rebuilt from the level stream and kept only when it matches the recorded top of book; competing Σ Q_min **bracketed** from the side totals (low max(T/4, x/3), high m + (x − m)/3, central the midpoint); our candidate quote inserted into the book and scored exactly; reward/day = pool × mean share. Cost of resting = stage-5 probes on the same market (filled 200-share amount × 60 s markout incl. rebate, × 1440, market × hour clusters), shown beside the reward with its 95 % bound, never netted on its point estimate for ranking. Screen: REST books of every paying market every 10 min; a standard two-sided quote; gross yield at a fixed 1k / 10k USD of capital; capacity at a 25 % share; markets ≥ 1 day from resolution; drop-outs counted. Results per day, headlined on the latest.
- **Why:** 11-06…09, 9 snapshots of 10,069 markets: 113k USD/day of pools, almost none in our families (BTC 15m and the ladder: none; Fed December: 700/day). Fed "+25 bp" on 11-09: 0.19¢ per share and side per day at the best price vs a fill P&L of −0.08 ± 0.55¢; "no change" 0.06¢ vs −0.56 ± 0.33¢ — the reward is below the fill-P&L uncertainty, no quote positive at 95 %. Universe: median gross yield at 1k from 1.1 %/day (weather) to 0.1 % (sports, finance), ~10× lower at 10k (thin pools). The top of the ranking is uncontested wide-spread and niche markets, where adverse selection is the open question.
- **Rejected:** `/rewards/markets/multi` (every market ever, 10+ min to page); yield at the minimum size (tiny pools show hundreds of % a day); ranking on reward + fill-P&L point estimate (picks noise); fees from tags alone; competing score ranges T/4…T/2 with central m + (x − m)/3 (the audit showed that central was the upper bound); pooling 11-06 (5× thinner book) with later days. Items 1–6 of the report's §4 came from an independent audit before commit.
- **Ref:** `src/polymm/rewards/`, `research/rewards.md`, `reports/rewards/summary.md`, `reports/rewards/rewards_params.json`, `tests/test_rewards.py`; dashboard section `rewards_*`; `ops/daily_refresh.sh` runs `polymm rewards … --sample 3` (step added by the D-030 session). Also in this commit: `dashboard.collection()` / `recorder_health()` scan the one store once and label the source by date (data/ → data_vps since D-030); `polymm dashboard --laptop-data` removed.
- **Status:** active (v1, D-027): gross yields outside our recorded markets; one day of pools and books. Next for rewards: record the books of the top screened markets and measure their fill P&L before calling any of them income.

### D-031 · 2026-11-10 · Latency budget measured: the 128 ms is Binance's leg; the simulator adds only the order path
- **Decision:** (1) Stage 5's 128 ms (`queue.engine.BASE_LAT_MS`) was labelled "median WS receive lag measured on the VPS". It is the **Binance** feed's lag (Tokyo → VPS). The report and the code comment are corrected; the number stays as a pessimistic stand-in for the order path, so no stage-5 result is rerun. (2) The replay runs on our receive clock, which already contains our feed lag, so the simulator (stage 7) adds **only the order path**: compute + VPS → CLOB send + venue processing, drawn per order from a distribution, base case centred at 75 ms (≈ 25 ms one way + 50 ms assumed venue time), bracketed by 30 ms and 128 ms, swept 0 / 30 / 75 / 128 / 300 / 1000 ms, and longer when the message rate is high (bursts are when the feed tail and jumps coincide). (3) `ops/feed_lag.py` measures the budget: feed lag per feed and day from the store, and the API round trip on one warm connection (run on the VPS). (4) The venue term is measured only with live orders (send → ack, send → our order seen in the WS book, cancel → size gone): first item of the stage-8 sim-vs-live comparison (D-028).
- **Why:** measured 2026-11-10 (VPS clock NTP-disciplined to ~0.3 ms), ms, p5 / p50 / p95 / p99:

  | Leg | 11-08 | 11-09 |
  |---|---|---|
  | Polymarket book changes → VPS | 10 / 12 / 61 / 3896 | 10 / 12 / 59 / 779 |
  | Polymarket top of book → VPS | 10 / 11 / 26 / 163 | 9 / 11 / 28 / 122 |
  | Polymarket trades → VPS | 10 / 12 / 33 / 198 | 10 / 11 / 43 / 176 |
  | Binance BTC trades → VPS | 121 / 128 / 155 / 338 | 120 / 127 / 136 / 159 |

  VPS → `clob.polymarket.com` (Cloudflare, Frankfurt edge) warm round trip: p50 46–52 ms, p95 64–65 ms; a new connection costs ~70 ms. TCP connect to Binance's stream host: 250 ms (one Tokyo round trip). So the book reaches us about as fast as it reaches anyone nearby; our slow leg is the BTC signal (~127 ms) and the burst tail of the book feed. Stage 5's latency sweep already shows the conclusions hold at 50 ms and 0 ms.
- **Levers, ranked for later (not decided):** a closer leading BTC price (Deribit / Coinbase / Bitstamp / Chainlink stream vs Binance, by lead–lag measured at our location); predicting risky moments instead of racing them; the burst tail (A/B feed arbitration, D-030); server region (time the API from a few cloud regions); warm connections and pre-signed orders; a faster language last.
- **Rejected:** keeping "128 ms = our WebSocket lag" (wrong leg); adding the feed lag on top of a receive-clock replay (counts it twice); a constant latency (hides the tail that coincides with jumps).
- **Ref:** `ops/feed_lag.py`, `src/polymm/queue/engine.py` (`BASE_LAT_MS`), `research/queue_fill.md` §1 correction, `research/quality_report.md` (Binance recv − event p50 128 ms), D-022, D-026, D-028, D-030
- **Status:** active

### D-032 · 2026-11-10 · Simulator v1: stage 5's queue engine run as a strategy, tick-level fair value, walk-forward ladder, Gate B FAIL on BTC 15m
- **Decision:** `polymm.sim` replays each market segment (strict clean window ∩ live window) event by event on our receive clock with our orders inserted:
  - **Queue model and clock:** stage 5's queue model in a compiled engine. Ties go market-before-ours, orders are post-only, cancels land after latency and never before their order, inventory carries across data gaps.
  - **Latency:** order-path only (D-031), 75 ms base × lognormal jitter (σ 0.126) plus the previous second's Polymarket feed lag above 12 ms.
  - **Fair value:** stage 3's walk-forward value at each second, moved between seconds by every Binance mid change through the BTC model, other inputs held.
  - **Strategy ladder:**
    - 0 symmetric on a 2 s timer;
    - 1 event-driven (stale cancel);
    - 2 reservation price $r = m - q\gamma\sigma_p^2(T-t)$, lean capped at 5 ¢, reduce-only at the end;
    - 3 jump guard;
    - 4 selective by stage 5's value table of earlier days;
    - 5 rewards on reward markets (Fed), scored after the run with stage 6's rules.
  - **Attribution per fill:** ① spread, ② adverse selection to 60 s, ③ to settlement, ④ rebate (+ rewards); ①+②+③ = $s(V-P)q$ exactly.
  - **Experiments:**
    - 78 settings on every day; walk-forward selection (day $k$ chooses on days $< k$), the last day a lockbox;
    - the selected settings rerun under four cancel models and six latencies (0–1000 ms);
    - deflated Sharpe over all trials, plateau test, windows needed.
  - **Gate B** = pessimistic out-of-sample lower 95 % > 0, lockbox mean > 0, DSR ≥ 0.95 and a plateau. The sanity checks (delay, random signal, do-nothing) veto it.
  - **Operations:** daily refresh step `polymm sim … --reuse`, which runs only the new day; earlier days' selections never change.
- **Why:**
  - **Data:** 11-06…09 (test 11-07/08, 163 windows; lockbox 11-09, 96).
  - **Gate B FAIL on every rung.** ② exceeds ① by 1.6–2.5 ¢ per filled share at 60 s on every rung, and the rebate is 0.29 ¢. USD per window (10 shares, matched, 75 ms):
    - rung 0: −3.41 ± 1.65;
    - rung 1: −5.30 ± 2.03;
    - rung 2: −5.04 ± 1.61;
    - rung 3 (jump guard): −0.52 ± 1.54, about 90 % of the loss removed;
    - rung 4: sits out (stage-5 table negative).
  - **Lockbox:** −2.27 / −1.85 / −1.20 / −1.18 / −0.44.
  - **Robustness:** the same under all four cancel models and on gap-free windows; DSR 0.00 with 78 trials.
  - **Latency:** rungs 1–2 lose more at 0 ms than at 75 ms. At 75 ms, post-only rejections filter out the price-improving orders that chase a fair-value tick; at 0 ms these land and are filled through, at −7 ¢. Within 2 SE.
  - **Fed rewards rung:** 0.24–0.51 USD/day rewards vs −14…+1.4 USD/day fill P&L.
  - **Checks:**
    - stage-5 probes replayed exactly (fill quantity and rejections 100 %, three cancel models);
    - identity error 1.8e-15;
    - fair at seconds = fair_oos exactly; tick formula self-check 4e-12;
    - do-nothing 0 fills; noise fair never better (4/4); 0 delay violations; 0 truncation;
    - 11 unit tests.
  - **Audit:** an independent audit before commit found the γ grid ~1000× too large (rerun), rung 5 joining a best bid above fair, rewards over covered minutes only (1.6× on 11-07), checks not vetoing the gate, a vacuous fair check, and silent truncation; all fixed.
- **Rejected:**
  - one simulated P&L number (D-028);
  - a constant latency;
  - re-implementing the queue model (the engine shares stage 5's rules and is checked against it);
  - selecting on all days;
  - an uncapped inventory lean;
  - averaging rewards over covered minutes only.
- **Ref:** `src/polymm/sim/`, `tests/test_sim.py`, `research/simulator.md`, `reports/sim/summary.md`, `reports/sim/sim.json`; dashboard keys `sim_*`; `ops/daily_refresh.sh` (sim step); D-024, D-026, D-027, D-028, D-029, D-031
- **Status:** active (v1, D-027): a floor for simple rules, not a verdict on BTC 15m. Next by the attribution: ② dominates, so a better jump/σ signal first, then the grid edge (wider h, more sensitive jump trigger), then more days.
