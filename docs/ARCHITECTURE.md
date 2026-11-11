# Target architecture: storage, research compute, live engine

> Status: **proposed, not yet decided** (agreed in discussion with Yehor on 2026-11-11). Each step
> below becomes a D-NNN entry in `docs/DECISIONS.md` when it is built. Dates and step status live
> in Notion (Global plan); this file holds the design and the measurements behind the dates.
> Prices and provider details must be verified before choosing.

## 1. Why (measured 2026-11-11)

**Memory.** `polymm fair` keeps every day's full 1 s grid in one table and copies it 3–4 times
(all days, held-out days, final fit). One day = 2.57 M rows, 1.85 GB in memory (ladder 80 %,
text IDs ~47 %). Peak memory footprint (`/usr/bin/time -l`, RAM + compressed + swap) on the
8 GB MacBook:

| step | 4 days | 5 days |
|---|---|---|
| `polymm fair` | 26.6 GB, ok | 30.0 GB, killed by the OS (the 11-11 refresh failure) |
| markouts `build_fair_oos` | – | 15.1 GB, ok |
| sim `fair_seconds` | – | 14.1 GB, ok |

markouts/sim grow ~3 GB footprint per day → same wall at ~9–10 days (≈ 11-15/16 refresh).
The fit really uses 0.89 M rows × 13 columns = 135 MB/day; scoring ~1.3 M rows ≈ 220 MB/day.
Running the 4- and 5-day reproductions froze the whole laptop ("out of application memory"):
heavy jobs do not belong on the machine people work on.

**Disk.** Laptop: 73 GB free of 460 GB. One full day adds ~6 GB:

| what | GB/day | role |
|---|---|---|
| `raw/ws_market` + `raw/binance` | 3.1 | original messages; backup for re-parsing (cold) |
| `events/` (book_events, tob, trades, …) | 1.6 | parsed data, read daily (hot) |
| `reports/fair/<day>/replay` | 1.2 | rebuilt books; rebuildable from events (warm) |
| spot, reference | ~0.1 | hot |

Keeping ~20 GB free for macOS (swap reached 15 GB on 11-11): tight around **11-20**, full ~11-23.
VPS: 38 GB disk, 32 GB free; keeps 7 days after a verified pull.

## 2. The memory fix (step 1, code only)

`polymm fair` (and markouts `fair_oos`; sim `fair_seconds` via the same helper, owned by the
strategy session) process **one day at a time**:

```
for each day:
    build the grid (~2 GB, temporary; cached to a temp file on disk)
    → write that day's outputs (fair.parquet)
    → keep the fit rows only: 15m + active ladder, 13 columns, integer IDs   (~85–135 MB/day)
    → keep the Stoikov table's count and sum per (family, spread, imbalance) bucket
    → keep the scoring sums per (group, cluster, candidate, horizon):
      n, Σz, Σy, Σz², Σzy, squared errors, hits, Brier sums                (a few MB/day)
    drop the day
fit on the kept rows; scores, calibration, ŵ and the shrink check from the pooled sums
```

Results stay the same up to floating-point rounding (~1e-12); the walk-forward fair series is
unchanged. Next knob when the fit rows get big (~1–2 months): cap the training window to the last N days.

**Built 2026-11-11 (D-035), measured at 5 days.** `polymm fair` 30.0 GB (killed) → ~6.9 GB;
markouts `fair_oos` 15.1 → 5.1 GB; sim `fair_seconds` (via the switch) 14.1 → 5.2 GB. The peak is
now set by the largest single day (2.6 M rows), not by the number of days. Two per-day hot spots
were also removed on the way: loading all 40–65 M top-of-book updates of a day (→ last update per
token-second, one hour at a time) and joins that copied the whole day. Above the first estimate of 3–4 GB:
the day itself, built and applied, still takes ~4.5–5 GB at peak; next levers if needed are
category ids for the long text ids (~half of a day's memory) or building the grid per family.

## 3. Target layout

```
 Polymarket WS / Binance
          │
   ┌──────▼───────┐      every hour       ┌─────────────────────────┐
   │ Recorder VPS │ ────────────────────► │  BUCKET (object storage)│
   │ small, 24/7  │                       │  raw/   (cold archive)  │
   │ keeps 7 days │                       │  events/ (read daily)   │
   └──────────────┘                       │  derived/ replay, grids │
                                          │  reports/ results       │
                                          └──────▲──────────┬───────┘
                          reads only needed      │          │ writes results back
                          columns, fast, free    │          │
                                          ┌──────┴──────────▼──────┐
                                          │ RESEARCH SERVER        │
                                          │ 64+ GB RAM, many cores │
                                          │ runs the daily refresh │
                                          └──────────┬─────────────┘
                                                     │ small results only (MB)
                                              ┌──────▼──────┐
                                              │ Laptop      │  code (git), Claude,
                                              │ (thin)      │  dashboard, reports
                                              └─────────────┘

 research server ──(fitted parameters)──► ENGINE (own small machine near the venue)
 ENGINE ──(log of every event / quote / order / fill / cancel)──► bucket ──► post-trade review
```

**Rule: move the compute to the data.** Reading the bucket from the laptop over home internet
(~150 GB/month of data) is slow and may cost egress; a server in the same datacenter reads it at
1–10 Gbit/s, usually free.

**Storage tiers.** Hot (read daily: events, spot, reference), warm (rebuildable: replay, grids),
cold (archive: raw). Object storage (S3-style: Hetzner Object Storage, Cloudflare R2, Backblaze
B2, AWS S3) lets polars read only the needed columns/row groups directly
(`pl.scan_parquet("s3://…/dt=…/*")`); a network disk (Hetzner Storage Box) is archive only.

## 4. What lives where

| | Laptop | Git host (private GitHub) | Bucket | Research server | Recorder VPS | Engine |
|---|---|---|---|---|---|---|
| Code | working copy (edit here) | **master copy + history** | – | pulled copy | pulled copy | tagged releases only |
| Data | none | none | **all** | reads, writes results | last 7 days | own logs, uploaded |
| Heavy compute | none | – | – | refresh, backtests, fits | – | live loop only |
| Results / dashboard | small copies | – | yes | writes | – | – |
| Secrets | SSH keys | never | – | storage keys | storage keys | **trading keys, only here** |
| Claude | yes | – | – | may work here over SSH | – | never |

Git holds what a human wrote (code, docs, decisions, setup scripts); never data, secrets or
generated files. Every report records the commit it was built from.

## 5. Workflows

**Research (daily).** Recorder uploads each finished hour to the bucket → research server runs
the refresh in the morning (quality/replay of the new day, then fair → markouts → queue →
rewards → sim, reading only needed columns) → results and dashboard data to the bucket → laptop
fetches only those. Interactive work: on the research server over SSH.

**Code changes.** Edit on the laptop → tests → commit → `git push` to the private remote →
deploy with a short script (`ops/deploy.sh <machine> [tag]`) that SSHes in, pulls, syncs the
environment, restarts. Rules per machine: research = pull latest anytime; recorder = rarely,
check its log after restart (data lost while it is down is lost for good); engine = tested,
tagged releases only, deployed between trading windows, one-command rollback to the previous tag.
No hand edits on servers: every machine is rebuilt from git + its setup script.

**Live engine (only after Gate B passes).** One event loop per event (book change, trade, Binance
quote, own fill): update state → recompute fair value incrementally → decide quotes (fair,
inventory skew, spread, guards) → risk checks (limits, max loss, stale feed, kill switch) →
send/cancel/replace only if needed → log in the background. Microseconds of compute; the network
dominates (D-031). Precomputed by research and loaded daily at a safe moment: Stoikov table,
β/half-life of the blend, σ model settings, quoting parameters. Computed live: book, mid,
imbalance, x_book, S and x_model, EWMA offset, live σ, inventory, quotes. Nothing slow
(database, fitting, history scans) on the hot path. Separate machine so research can never slow
or crash it; keys only there.

## 6. Migration steps and deadlines (set 2026-11-11 from the measurements above)

| # | step | why this date | deadline |
|---|---|---|---|
| 1 | Memory fix (§2) in `polymm fair` + markouts `fair_oos`; hand the strategy session the switch for sim `fair_seconds` | `fair` fails every morning now; markouts/sim hit the wall ≈ 11-15/16 refresh | **2026-11-14** (sim switch by the strategy session, same date) |
| 0 | Private git remote (GitHub); push `main` | code and decision history exist only on the laptop; every later step pulls from it | **2026-11-14** |
| 2 | Bucket + VPS uploads hourly; laptop stops pulling `raw/`; old local `raw/` moved to the bucket and deleted locally after a verified copy | laptop disk tight ≈ 11-20 | **2026-11-18** |
| 2b | (optional) keep only recent `replay/` folders locally, rebuild on demand; check first which steps read old replays (queue `--reuse`) | +1.2 GB/day saved | with step 2 |
| 3 | Research server; daily refresh runs there reading the bucket; laptop keeps code + reports only | after step 2 the laptop still grows ~3 GB/day → tight again ≈ early December | **2026-12-05** (start by 12-01) |
| 4 | Live engine on its own small machine near the venue | only after Gate B passes; check Austria geo-restriction first | no date |

Open before choosing: provider prices (bucket, research server), venue server location (measure,
as in D-031), whether the research server is always on or started on demand.
