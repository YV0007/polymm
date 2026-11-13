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
# Why --partial-dir and not --partial: --partial leaves an interrupted file under its final
# name, so a reader sees a truncated Parquet file (2026-11-10: the server stopped answering
# mid-pull and the morning refresh crashed on a 64 KB stub). --partial-dir keeps the piece in a
# hidden .rsync-partial/ folder until it is complete; the next attempt resumes from it.
# Why --timeout and keepalives: a stalled link must fail in minutes, not hang; then retry.
ok=0
for attempt in 1 2 3; do
  # Why --exclude feeds: the external feeds (D-042) go VPS -> bucket; the laptop fetches
  # their tables from the bucket on demand (polymm-feeds fetch), never the raw stream.
  if rsync -a --partial-dir=.rsync-partial --timeout=120 --exclude '*.tmp' --exclude '.rsync-partial' --exclude 'feeds' --stats \
       -e "ssh -o ServerAliveInterval=15 -o ServerAliveCountMax=4 -o ConnectTimeout=20" \
       "polymm@$HOST:$REMOTE/" "$DEST/" | grep -E "Number of .*files transferred|Total transferred"; then
    ok=1; break
  fi
  echo "WARNING: pull attempt $attempt failed; retrying in $((attempt * 30)) s"
  sleep $((attempt * 30))
done
if [ "$ok" != 1 ]; then echo "ERROR: pull failed after 3 attempts"; exit 1; fi

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
  local_=$( (cd "$DEST/$p" 2>/dev/null && find . -path '*/.rsync-partial' -prune -o -type f -exec stat -f '%N %z' {} + | sed 's|^\./||' | sort) || true)
  # Why "every server file is here with the same size" and not "the two lists are equal":
  # the local store may hold more than the server (laptop-only hours merged in, D-030);
  # what matters before deleting on the server is that nothing of the server's is missing.
  missing=$(comm -23 <(printf '%s\n' "$remote") <(printf '%s\n' "$local_") | grep -c . || true)
  if [ -z "$remote" ]; then
    echo "KEEP  $p: only unfinished (.tmp) files on server -> left for a human to look at"
    continue
  elif [ "$missing" != 0 ]; then
    echo "KEEP  $p: $missing server files missing or different locally -> not deleted"
    continue
  else
    echo "prune $p: $(wc -l <<<"$remote" | tr -d ' ') files verified locally"
  fi
  [ "$DRY_RUN" = 1 ] || ssh -n "$HOST" "rm -rf $REMOTE/$p"
done <<<"$partitions"
ssh -n "$HOST" "df -h /mnt/HC_Volume_* | tail -1 | awk '{print \"server volume: \"\$3\" used / \"\$4\" free\"}'"
echo "== $(date -u +%FT%TZ) done"
