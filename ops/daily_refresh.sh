#!/bin/bash
# Daily research refresh (run by the morning scheduled task, or by hand):
#   1. pull the server's data (incremental; prunes only verified copies older than 7 days),
#      then check every Parquet file is complete; stop on either failure
#   2. for every UTC day from FIRST to yesterday without a .complete marker:
#        polymm quality (replay + checks + strict clean windows) and polymm vol (σ series)
#   3. polymm fair over all days (walk-forward; refreshes the Gamma strikes/outcomes)
#   4. polymm markouts over all days
#   5. polymm queue over all days (new days only; --reuse), polymm rewards, polymm sim (--reuse)
#   6. polymm dashboard  -> reports/dashboard/dashboard_data.json (the dashboard's one data file)
# Why plots are off: the reports' PNGs live in research/ (tracked); a daily run must not
# rewrite tracked files. Rerun with plots by hand when a write-up is refreshed.
# Why a .complete marker: a day is final once yesterday has ended and been pulled; a day run
# earlier on partial data (no marker) is redone in full.
set -uo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs
LOG=logs/daily_refresh.log
exec > >(tee -a "$LOG") 2>&1
P=.venv/bin/polymm
FIRST=${FIRST:-2026-11-06}
# One store since 2026-11-10 (D-030): data_vps/ holds the laptop recording before the server's
# first instant (2026-11-07 09:35:45 UTC) and the server's after; data/ is a symlink to it.
echo "== $(date -u +%FT%TZ) start"

# Why stop on a failed pull or a damaged file: on 2026-11-10 an interrupted pull left a
# truncated Parquet file and every step after it failed; a run on incomplete data would at
# best waste 30 minutes and at worst publish numbers from half a day. Stopping keeps the
# dashboard on yesterday's complete numbers; the next run catches up.
if ! bash ops/nightly_pull.sh; then
  echo "ERROR: pull failed (see logs/nightly_pull.log); stopping, nothing recomputed"
  exit 1
fi
if ! .venv/bin/python -I ops/check_parquet.py data_vps; then
  echo "ERROR: damaged Parquet files in the store (listed above); stopping, nothing recomputed"
  exit 1
fi

YDAY=$(date -u -v-1d +%F)
DATES=()
d=$FIRST
while [[ ! "$d" > "$YDAY" ]]; do DATES+=("$d"); d=$(date -u -j -f %F -v+1d "$d" +%F); done
echo "days: ${DATES[*]}"

for d in "${DATES[@]}"; do
  out=reports/fair/$d
  [[ -f $out/.complete ]] && continue
  data=data_vps
  echo "-- $d: quality + vol on $data"
  rm -rf "$out/replay"   # rebuild from the full day, not a partial earlier run
  if $P quality --date "$d" --data "$data" --out "$out" | tail -3 \
     && $P vol --date "$d" --data "$data" --spot-data data_vps --replay-out "$out/replay" \
          --strict "$out/clean_windows_strict.parquet" --out "$out/vol" --no-plots | tail -2; then
    touch "$out/.complete"
  else
    # Why stop here: fair, markouts and queue need every day's replay and clean windows;
    # with one missing they would all fail and say less about why.
    echo "ERROR: $d quality/vol failed; stopping before fair/markouts/queue"
    exit 1
  fi
done

echo "-- fair"
$P fair "${DATES[@]}" --no-plots 2>&1 | grep -v "Sortedness\|join_asof" | tail -2 || echo "ERROR: fair failed"
echo "-- markouts"
$P markouts "${DATES[@]}" --no-plots 2>&1 | tail -2 || echo "ERROR: markouts failed"
echo "-- queue"
# Why --reuse: probes.parquet of a finished day never changes, and a day costs ~4 min; only new
# days are built, the pooled tables are recomputed over all of them (D-026).
$P queue "${DATES[@]}" --reuse --no-plots 2>&1 | tail -2 || echo "ERROR: queue failed"
echo "-- rewards"
# Why --sample 3: only today's reward configs/books are published; 3 REST snapshots 10 min apart
# (~1 min of requests) keep the screen and the daily config snapshot (reference/rewards/dt=…)
# current (D-029). Why the guard: the step switches itself on once `polymm rewards` is
# committed, so the morning run never fails on a command that does not exist yet.
if [[ "${SKIP_REWARDS:-0}" != 1 ]] && $P rewards --help >/dev/null 2>&1; then
  $P rewards "${DATES[@]}" --sample 3 --every 600 --no-plots 2>&1 | tail -2 || echo "ERROR: rewards failed"
else
  echo "rewards: skipped (SKIP_REWARDS=1 or the command is not available yet)"
fi
echo "-- sim"
# Why --reuse: a day's grid and its walk-forward band depend only on that day and earlier ones, so
# finished days are read back and only the new day runs (~10 min); the last day becomes the new
# lockbox (D-032). Same guard as rewards: on once `polymm sim` exists.
if [[ "${SKIP_SIM:-0}" != 1 ]] && $P sim --help >/dev/null 2>&1; then
  $P sim "${DATES[@]}" --reuse --no-plots 2>&1 | grep -v "Sortedness\|join_asof" | tail -2 || echo "ERROR: sim failed"
else
  echo "sim: skipped (SKIP_SIM=1 or the command is not available yet)"
fi
echo "-- dashboard"
$P dashboard | tail -1 || echo "ERROR: dashboard failed"
echo "== $(date -u +%FT%TZ) done"
