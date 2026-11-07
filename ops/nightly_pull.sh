#!/bin/bash
# Nightly (00:00 Europe/Vienna via launchd): pull all data from the VPS, then
# delete day partitions on the VPS that are older than KEEP_DAYS *and* present
# locally file-for-file with identical sizes. Nothing is deleted on any doubt.
#   ops/nightly_pull.sh [host] [dest]        env: KEEP_DAYS=7 DRY_RUN=1
# Why verify by listing, not by trusting rsync's exit code: rsync can finish
# fine while a day's last hour is still being written on the server; that day
# is pulled again the next night and only pruned once both sides agree.
set -euo pipefail
HOST=${1:-polymm}
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST=${2:-$ROOT/data_vps}
KEEP_DAYS=${KEEP_DAYS:-7}
DRY_RUN=${DRY_RUN:-0}
REMOTE=/home/polymm/polymm/data
LOG="$ROOT/logs/nightly_pull.log"
mkdir -p "$DEST" "$ROOT/logs"
exec >>"$LOG" 2>&1
echo "== $(date -u +%FT%TZ) start host=$HOST dest=$DEST keep=${KEEP_DAYS}d dry=$DRY_RUN"

# 1) pull (incremental, resumable; files still being written are skipped)
rsync -a --partial --exclude '*.tmp' --stats "polymm@$HOST:$REMOTE/" "$DEST/" | grep -E "Number of .*files transferred|Total transferred"

# 2) dead-man check: yesterday (UTC) must have book_events locally after the pull
Y=$(date -u -v-1d +%F)
if [ -z "$(ls "$DEST/events/book_events/dt=$Y" 2>/dev/null)" ]; then
  echo "WARNING: no book_events for dt=$Y after pull -> recorder on $HOST may be down"
fi

# 3) prune: every day partition older than the cutoff, in all day-partitioned layers
CUTOFF=${CUTOFF:-$(date -u -v-"${KEEP_DAYS}"d +%F)}  # env CUTOFF=YYYY-MM-DD overrides (testing only)
# Why the guard: a partition still being written can pass the listing check in
# the instant between pull and compare; only finished days (< yesterday) qualify.
if [ "$DRY_RUN" != 1 ] && ! [[ "$CUTOFF" < "$(date -u -v-1d +%F)" ]]; then
  echo "refusing to prune: cutoff $CUTOFF is not before yesterday"; exit 1
fi
# Why `ssh -n` inside the loop: ssh reads stdin by default and would eat the
# rest of the partition list after the first iteration.
partitions=$(ssh -n "$HOST" "cd $REMOTE && find raw events spot -mindepth 2 -maxdepth 2 -type d -name 'dt=*' | sort")
while read -r p; do
  [ -n "$p" ] || continue
  dt=${p##*dt=}
  [[ "$dt" < "$CUTOFF" ]] || continue
  remote=$(ssh -n "$HOST" "cd $REMOTE/$p && find . -type f ! -name '*.tmp' -printf '%P %s\n' | sort")
  local_=$( (cd "$DEST/$p" 2>/dev/null && find . -type f -exec stat -f '%N %z' {} + | sed 's|^\./||' | sort) || true)
  if [ -z "$remote" ]; then
    echo "KEEP  $p: only unfinished (.tmp) files on server -> left for a human to look at"
    continue
  elif [ "$remote" != "$local_" ]; then
    echo "KEEP  $p: local copy differs ($(wc -l <<<"$remote") remote vs $(wc -l <<<"$local_") local files) -> not deleted"
    continue
  else
    echo "prune $p: $(wc -l <<<"$remote" | tr -d ' ') files verified locally"
  fi
  [ "$DRY_RUN" = 1 ] || ssh -n "$HOST" "rm -rf $REMOTE/$p"
done <<<"$partitions"
ssh -n "$HOST" "df -h /mnt/HC_Volume_* | tail -1 | awk '{print \"server volume: \"\$3\" used / \"\$4\" free\"}'"
echo "== $(date -u +%FT%TZ) done"
