#!/bin/zsh
# Burst server: a Hetzner Cloud machine rented by the hour for jobs the laptop cannot hold,
# created from the laptop, used, deleted (D-043). Nothing on it is permanent: code comes from
# `git archive HEAD`, data from the bucket, results are pulled back into reports/.
#
#   ops/burst.sh up [--type ccx43] [--ttl 8]      create + bootstrap (uv, code, s3.env); ~2 min
#   ops/burst.sh fetch <prefix> [<prefix>...]     bucket → server data/ (e.g. feeds/tables/binance_perp_bbo/dt=2026-11-13)
#   ops/burst.sh run '<command>'                  run in /root/polymm (foreground, ssh -t)
#   ops/burst.sh bg '<command>'                   same, detached; `ops/burst.sh log` tails it
#   ops/burst.sh pull                             server reports/ → laptop reports/ (adds, never deletes)
#   ops/burst.sh down                             delete the server (the bill stops here)
#   ops/burst.sh status                           what is running, for how long, estimated cost
#   ops/burst.sh job '<command>' [prefix...]      up → fetch → run → pull → down, one shot
#
# Why a separate Hetzner project and token: the token lives on the laptop (~/.config/polymm/
# hcloud.env) and, for the TTL guard, on the burst server itself; a token that can only touch
# the burst project can never delete the recorder VPS or the bucket.
# Why a TTL: a powered-off cloud server is still billed; the only way to stop paying is delete.
# cloud-init installs a timer that deletes the server through the API after --ttl hours even
# if the laptop is asleep or `down` was forgotten. Default 8 h ≈ €3.
# Why ccx43 (16 dedicated vCPU, 64 GB, 360 GB NVMe, fsn1 = the bucket's region): the daily
# refresh peaks at 7 GB and a day of perp best-bid/ask is 3 GB in memory; 64 GB holds a week
# of it. €0.38/h (verified 2026-11-13); a nightly refresh is ~20 min ≈ €0.15.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV=~/.config/polymm/hcloud.env
NAME=${BURST_NAME:-polymm-burst}
PRICE_PER_H=0.38
[[ -f $ENV ]] || { echo "missing $ENV (HCLOUD_TOKEN=... from the Hetzner console, project polymm-burst)"; exit 1 }
export HCLOUD_TOKEN=$(grep ^HCLOUD_TOKEN= $ENV | cut -d= -f2-)
cmd=${1:-status}; shift || true

ip() { hcloud server ip "$NAME" 2>/dev/null || true }
ssh_() { ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -i ~/.ssh/id_ed25519_hetzner "root@$(ip)" "$@" }

case $cmd in
  up)
    TYPE=ccx43; TTL=8
    while [[ $# -gt 0 ]]; do case $1 in --type) TYPE=$2; shift 2;; --ttl) TTL=$2; shift 2;; *) echo "unknown $1"; exit 1;; esac; done
    if [[ -n "$(ip)" ]]; then echo "$NAME already exists at $(ip)"; exit 0; fi
    hcloud ssh-key describe laptop >/dev/null 2>&1 || hcloud ssh-key create --name laptop --public-key-from-file ~/.ssh/id_ed25519_hetzner.pub
    # cloud-init: uv for root, rsync, and the TTL self-delete timer (the token file is 600 root)
    USERDATA=$(mktemp); trap 'rm -f "$USERDATA"' EXIT
    cat > "$USERDATA" <<EOF
#cloud-config
package_update: true
packages: [rsync, curl, zstd, htop]
write_files:
  - path: /root/.config/polymm/hcloud.env
    permissions: "0600"
    content: |
      HCLOUD_TOKEN=$HCLOUD_TOKEN
  - path: /usr/local/bin/burst-self-delete
    permissions: "0755"
    content: |
      #!/bin/bash
      # Why: a forgotten burst server costs €9/day; this deletes it after the TTL, whatever the laptop does
      TOKEN=\$(grep ^HCLOUD_TOKEN= /root/.config/polymm/hcloud.env | cut -d= -f2-)
      ID=\$(curl -s http://169.254.169.254/hetzner/v1/metadata/instance-id)
      curl -s -X DELETE -H "Authorization: Bearer \$TOKEN" https://api.hetzner.cloud/v1/servers/\$ID
  - path: /etc/systemd/system/burst-ttl.timer
    content: |
      [Unit]
      Description=delete this burst server after its TTL
      [Timer]
      OnBootSec=${TTL}h
      [Install]
      WantedBy=timers.target
  - path: /etc/systemd/system/burst-ttl.service
    content: |
      [Service]
      Type=oneshot
      ExecStart=/usr/local/bin/burst-self-delete
runcmd:
  - systemctl daemon-reload
  - systemctl enable --now burst-ttl.timer
  - curl -LsSf https://astral.sh/uv/install.sh | sh
  - mkdir -p /root/polymm /root/polymm/data
  - ln -sfn /root/polymm/data /root/polymm/data_vps
  - touch /root/.bootstrapped
EOF
    echo "creating $NAME ($TYPE, fsn1, ubuntu-24.04, ttl ${TTL}h)…"
    hcloud server create --name "$NAME" --type "$TYPE" --image ubuntu-24.04 --location fsn1 --ssh-key laptop --user-data-from-file "$USERDATA" --label project=polymm >/dev/null
    date -u +%FT%TZ > ~/.config/polymm/burst.started
    echo "waiting for cloud-init…"
    for i in {1..60}; do ssh_ 'test -f /root/.bootstrapped' 2>/dev/null && break; sleep 5; done
    ssh_ 'test -f /root/.bootstrapped' || { echo "bootstrap did not finish; check: ops/burst.sh run \"cloud-init status --long\""; exit 1 }
    # code: committed tree only, same rule as deploy.sh (D-042); secrets: s3.env only
    STAGE=$(mktemp -d)
    git -C "$ROOT" archive HEAD | tar -x -C "$STAGE"
    git -C "$ROOT" rev-parse HEAD > "$STAGE/DEPLOYED_COMMIT"
    rsync -az --delete --exclude data --exclude data_vps --exclude logs --exclude .venv --exclude reports --exclude '__pycache__' \
      -e "ssh -i ~/.ssh/id_ed25519_hetzner -o StrictHostKeyChecking=accept-new" "$STAGE/" "root@$(ip):/root/polymm/"
    rm -rf "$STAGE"
    scp -q -i ~/.ssh/id_ed25519_hetzner ~/.config/polymm/s3.env "root@$(ip):/root/.config/polymm/s3.env"
    ssh_ 'chmod 600 /root/.config/polymm/s3.env; cd /root/polymm && mkdir -p logs reports && ~/.local/bin/uv sync --frozen -q && echo "uv ok: $(.venv/bin/python -V)"'
    echo "$NAME ready at $(ip): $(ssh_ 'echo "$(nproc) vCPU, $(free -g | awk "NR==2{print \$2}") GB"'), deletes itself in ${TTL}h; ops/burst.sh down when finished"
    ;;
  fetch)
    [[ $# -gt 0 ]] || { echo "usage: ops/burst.sh fetch <prefix>..."; exit 1 }
    args=(); for p in "$@"; do args+=(--prefix "$p"); done
    ssh_ -t "cd /root/polymm && .venv/bin/polymm-feeds fetch ${args[*]} --root data"
    ;;
  run)   ssh_ -t "cd /root/polymm && $*" ;;
  bg)    ssh_ "cd /root/polymm && nohup bash -lc '$*' > logs/bg.log 2>&1 & echo started; sleep 1; tail -n 2 logs/bg.log" ;;
  log)   ssh_ "tail -n ${1:-30} -f /root/polymm/logs/bg.log" ;;
  pull)  rsync -az -e "ssh -i ~/.ssh/id_ed25519_hetzner" "root@$(ip):/root/polymm/reports/" "$ROOT/reports/" && echo "reports pulled" ;;
  down)
    [[ -n "$(ip)" ]] || { echo "no $NAME running"; exit 0 }
    hcloud server delete "$NAME" >/dev/null && echo "$NAME deleted"
    if [[ -f ~/.config/polymm/burst.started ]]; then
      s=$(date -j -u -f %FT%TZ "$(cat ~/.config/polymm/burst.started)" +%s 2>/dev/null || echo 0); h=$(( ($(date +%s) - s + 3599) / 3600 ))
      echo "ran ~${h} h ≈ €$(printf %.2f $((h * PRICE_PER_H)))"; rm -f ~/.config/polymm/burst.started
    fi
    ;;
  status)
    hcloud server list -o columns=name,status,type,ipv4,created,age
    if [[ -n "$(ip)" && -f ~/.config/polymm/burst.started ]]; then
      s=$(date -j -u -f %FT%TZ "$(cat ~/.config/polymm/burst.started)" +%s 2>/dev/null || echo 0); h=$(( ($(date +%s) - s + 3599) / 3600 ))
      echo "$NAME up ~${h} h ≈ €$(printf %.2f $((h * PRICE_PER_H))) so far (ops/burst.sh down to stop the bill)"
    fi
    ;;
  job)
    [[ $# -gt 0 ]] || { echo "usage: ops/burst.sh job '<command>' [prefix...]"; exit 1 }
    JOB=$1; shift
    trap '"$0" down' EXIT   # Why: a failed job must not leave a €9/day server behind
    "$0" up
    [[ $# -gt 0 ]] && "$0" fetch "$@"
    "$0" run "$JOB"
    "$0" pull
    ;;
  *) echo "unknown command $cmd"; exit 1 ;;
esac
