#!/bin/zsh
# Push the code (never data) from this Mac to the VPS and restart the recorder.
#   ops/deploy.sh <server-ip>            first time: then run ops/vps_setup.sh as root
#   ops/deploy.sh <server-ip> restart    later code updates
# Why rsync, not git clone: no GitHub remote or deploy key needed; the Mac's
# working tree is the source of truth. Run it from a committed tree.
set -euo pipefail
HOST=${1:?usage: ops/deploy.sh <server-ip> [restart]}
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
USER_AT=root@$HOST
ssh "$USER_AT" 'id polymm >/dev/null 2>&1 || useradd -m -s /bin/bash polymm; install -d -o polymm -g polymm /home/polymm/polymm'
rsync -az --delete \
  --exclude data --exclude data_vps --exclude .claude --exclude .git --exclude logs --exclude .venv --exclude reports --exclude '__pycache__' --exclude .DS_Store \
  "$ROOT/" "$USER_AT:/home/polymm/polymm/"
ssh "$USER_AT" 'chown -R polymm:polymm /home/polymm/polymm'
if [[ "${2:-}" == restart ]]; then
  ssh "$USER_AT" 'sudo -u polymm bash -lc "cd ~/polymm && ~/.local/bin/uv sync --frozen" && systemctl restart polymm && sleep 10 && tail -n 2 /home/polymm/polymm/logs/recorder.log'
fi
echo "deployed to $HOST"
