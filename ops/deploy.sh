#!/bin/zsh
# Push the code (never data) from this Mac to the VPS and restart the recorder.
#   ops/deploy.sh <server-ip>            first time: then run ops/vps_setup.sh as root
#   ops/deploy.sh <server-ip> restart    later code updates
# Why rsync, not git clone: no GitHub remote or deploy key needed. Why `git archive HEAD` and
# not the working tree (D-042): other chats keep uncommitted edits in the tree; a server only
# ever gets committed code, so what runs there is always a commit you can name.
set -euo pipefail
HOST=${1:?usage: ops/deploy.sh <server-ip|ssh-alias> [restart]}
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
USER_AT=root@$HOST
STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
git -C "$ROOT" archive HEAD | tar -x -C "$STAGE"
git -C "$ROOT" rev-parse HEAD > "$STAGE/DEPLOYED_COMMIT"
ssh "$USER_AT" 'id polymm >/dev/null 2>&1 || useradd -m -s /bin/bash polymm; install -d -o polymm -g polymm /home/polymm/polymm'
rsync -az --delete \
  --exclude data --exclude data_vps --exclude logs --exclude .venv --exclude reports --exclude '__pycache__' \
  "$STAGE/" "$USER_AT:/home/polymm/polymm/"
ssh "$USER_AT" 'chown -R polymm:polymm /home/polymm/polymm'
if [[ "${2:-}" == restart ]]; then
  ssh "$USER_AT" 'sudo -u polymm bash -lc "cd ~/polymm && ~/.local/bin/uv sync --frozen" && systemctl restart polymm && sleep 10 && tail -n 2 /home/polymm/polymm/logs/recorder.log'
elif [[ "${2:-}" == sync ]]; then
  # Why a sync-only mode: the feeds service and the timers pick up new code on their own
  # restart; the Polymarket recorder is restarted only on purpose (data lost while down is lost)
  ssh "$USER_AT" 'sudo -u polymm bash -lc "cd ~/polymm && ~/.local/bin/uv sync --frozen"'
fi
echo "deployed to $HOST"
