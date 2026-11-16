#!/usr/bin/env bash
# Run on the Ubuntu server from the folder that contains ./publish and ./deploy
set -euo pipefail

sudo useradd --system --no-create-home --shell /usr/sbin/nologin refrilogger 2>/dev/null || true
sudo mkdir -p /opt/refrilogger /etc/refrilogger
sudo cp -r publish/. /opt/refrilogger/
sudo chown -R root:root /opt/refrilogger
sudo chmod +x /opt/refrilogger/refrilogger
sudo cp deploy/refrilogger.service /etc/systemd/system/refrilogger.service
[ -f /etc/refrilogger/refrilogger.env ] || sudo install -m 640 -o root -g refrilogger /dev/null /etc/refrilogger/refrilogger.env

sudo systemctl daemon-reload
sudo systemctl enable --now refrilogger
systemctl --no-pager status refrilogger || true
