#!/bin/zsh
# Copy finished data files from the VPS to this Mac (incremental, resumable).
#   ops/pull_data.sh <server-ip> [dest]     dest defaults to ./data_vps
# Why a separate dest during the overlap week: laptop and VPS both write
# part-HHMMSS-seq files; keeping them apart lets us compare the two audits.
# After cut-over, pull into ./data.
# Why --exclude '*.tmp': files still being written are skipped and arrive next run.
set -euo pipefail
HOST=${1:?usage: ops/pull_data.sh <server-ip> [dest]}
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST=${2:-$ROOT/data_vps}
mkdir -p "$DEST"
rsync -a --partial --stats --exclude '*.tmp' "polymm@$HOST:/home/polymm/polymm/data/" "$DEST/"
