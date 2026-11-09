#!/bin/bash
# Daily research refresh (run by the morning scheduled task, or by hand):
#   1. pull the server's data (incremental; prunes only verified copies older than 7 days)
#   2. for every UTC day from FIRST to yesterday without a .complete marker:
#        polymm quality (replay + checks + strict clean windows) and polymm vol (σ series)
#   3. polymm fair over all days (walk-forward; refreshes the Gamma strikes/outcomes)
#   4. polymm markouts over all days
#   5. polymm dashboard  -> reports/dashboard/dashboard_data.json (the dashboard's one data file)
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
SERVER_FROM=2026-11-07   # earlier days exist only in the laptop recording (data/)
echo "== $(date -u +%FT%TZ) start"

bash ops/nightly_pull.sh || echo "WARN: pull failed; continuing with what is local"

YDAY=$(date -u -v-1d +%F)
DATES=()
d=$FIRST
while [[ ! "$d" > "$YDAY" ]]; do DATES+=("$d"); d=$(date -u -j -f %F -v+1d "$d" +%F); done
echo "days: ${DATES[*]}"

for d in "${DATES[@]}"; do
  out=reports/fair/$d
  [[ -f $out/.complete ]] && continue
  data=data_vps; [[ "$d" < "$SERVER_FROM" ]] && data=data
  echo "-- $d: quality + vol on $data"
  rm -rf "$out/replay"   # rebuild from the full day, not a partial earlier run
  if $P quality --date "$d" --data "$data" --out "$out" | tail -3 \
     && $P vol --date "$d" --data "$data" --spot-data data_vps --replay-out "$out/replay" \
          --strict "$out/clean_windows_strict.parquet" --out "$out/vol" --no-plots | tail -2; then
    touch "$out/.complete"
  else
    echo "ERROR: $d quality/vol failed"
  fi
done

echo "-- fair"
$P fair "${DATES[@]}" --no-plots 2>&1 | grep -v "Sortedness\|join_asof" | tail -2 || echo "ERROR: fair failed"
echo "-- markouts"
$P markouts "${DATES[@]}" --no-plots 2>&1 | tail -2 || echo "ERROR: markouts failed"
echo "-- dashboard"
$P dashboard | tail -1 || echo "ERROR: dashboard failed"
echo "== $(date -u +%FT%TZ) done"
