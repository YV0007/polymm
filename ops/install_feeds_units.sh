#!/bin/bash
# Install / refresh the feeds units on the VPS (run as root, after ops/deploy.sh):
#   ssh root@<ip> 'bash /home/polymm/polymm/ops/install_feeds_units.sh'
# Idempotent. Needs ~/.config/polymm/{s3.env,polybolt.env} for the polymm user (put there by hand).
set -euo pipefail
cd /home/polymm/polymm
for u in polymm-feeds.service polymm-feeds-upload.service polymm-feeds-upload.timer polymm-feeds-parse.service polymm-feeds-parse.timer; do
  cp "ops/$u" "/etc/systemd/system/$u"
done
# Why: same rule as polymm.service (D-022): no auto-restart by unattended-upgrades
cat > /etc/needrestart/conf.d/polymm-feeds.conf <<'NR'
$nrconf{override_rc}{qr(^polymm-feeds\.service$)} = 0;
NR
systemctl daemon-reload
systemctl enable --now polymm-feeds-upload.timer polymm-feeds-parse.timer
systemctl enable polymm-feeds.service
systemctl restart polymm-feeds.service
sleep 20
systemctl --no-pager status polymm-feeds | head -5
systemctl list-timers --no-pager | grep polymm
