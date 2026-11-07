#!/bin/bash
# One-time server setup. Run ON the VPS as root, after ops/deploy.sh copied the repo:
#   ssh root@<ip> 'bash /home/polymm/polymm/ops/vps_setup.sh'
# Idempotent: safe to run again.
set -euo pipefail

# ---- OS basics -------------------------------------------------------------------
timedatectl set-timezone UTC
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq chrony unattended-upgrades rsync curl
# Why chrony: ts_recv_ns is our clock; latency = ts_recv - ts_exch is only
# meaningful if the server clock is disciplined to ms level.
systemctl enable --now chrony

# ---- service user (no password, key login only) ------------------------------------
id polymm >/dev/null 2>&1 || useradd -m -s /bin/bash polymm
install -d -m 700 -o polymm -g polymm /home/polymm/.ssh
cp /root/.ssh/authorized_keys /home/polymm/.ssh/authorized_keys
chown polymm:polymm /home/polymm/.ssh/authorized_keys
# Why a drop-in: sshd takes the first value it reads and sshd_config.d/ is read
# before the main file, so editing sshd_config alone can be silently overridden.
printf 'PasswordAuthentication no\nPermitRootLogin prohibit-password\n' > /etc/ssh/sshd_config.d/00-polymm.conf
systemctl reload ssh || systemctl reload sshd

# ---- data on the attached Volume ---------------------------------------------------
VOL=$(ls -d /mnt/HC_Volume_* 2>/dev/null | head -1 || true)
if [ -n "$VOL" ]; then
  # Why: a Volume resized in the console grows the disk, not the filesystem.
  resize2fs "$(findmnt -n -o SOURCE "$VOL")" >/dev/null 2>&1 || true
  install -d -o polymm -g polymm "$VOL/polymm-data" "$VOL/polymm-logs"
  # Why symlinks: config.yaml stays identical to the laptop's (data/, logs/).
  ln -sfn "$VOL/polymm-data" /home/polymm/polymm/data
  ln -sfn "$VOL/polymm-logs" /home/polymm/polymm/logs
  echo "data -> $VOL/polymm-data"
else
  echo "WARNING: no Hetzner Volume mounted; data stays on the root disk"
fi
chown -R polymm:polymm /home/polymm/polymm

# ---- python env via uv -------------------------------------------------------------
sudo -u polymm bash -lc 'command -v ~/.local/bin/uv >/dev/null || curl -LsSf https://astral.sh/uv/install.sh | sh'
sudo -u polymm bash -lc 'cd ~/polymm && ~/.local/bin/uv sync --frozen'

# ---- service -----------------------------------------------------------------------
cp /home/polymm/polymm/ops/polymm.service /etc/systemd/system/polymm.service
systemctl daemon-reload
systemctl enable --now polymm
sleep 15
systemctl --no-pager status polymm | head -5
tail -n 2 /home/polymm/polymm/logs/recorder.log || true

# ---- nightly Binance history backfill (yesterday's daily zip appears ~02:00 UTC) ----
# Why cron, not a recorder task: it is a one-shot download that must survive
# recorder restarts and is easier to re-run by hand (`polymm backfill-spot DATE`).
CRON_LINE='30 3 * * * cd /home/polymm/polymm && /home/polymm/.local/bin/uv run --frozen polymm backfill-spot $(date -u -d yesterday +\%F) >> /home/polymm/polymm/logs/backfill-cron.log 2>&1'
if ! crontab -u polymm -l 2>/dev/null | grep -q 'polymm backfill-spot'; then
  (crontab -u polymm -l 2>/dev/null; echo "$CRON_LINE") | crontab -u polymm -
  echo "installed nightly backfill-spot cron for polymm"
fi
