#!/bin/zsh
# Install / control the recorder as a launchd user agent.
#   ops/launchd.sh install   copy plist, load, start
#   ops/launchd.sh stop      SIGTERM -> clean flush, then unload (no auto-restart)
#   ops/launchd.sh start     load again
#   ops/launchd.sh status
# Second argument picks the agent: com.polymm.recorder (default) | com.polymm.nightly-pull
set -e
LABEL=${2:-com.polymm.recorder}
SRC="$(cd "$(dirname "$0")" && pwd)/$LABEL.plist"
DST="$HOME/Library/LaunchAgents/$LABEL.plist"
case "$1" in
  install) mkdir -p "$HOME/Library/LaunchAgents" "$(dirname "$SRC")/../logs"; cp "$SRC" "$DST"; launchctl bootstrap "gui/$(id -u)" "$DST" 2>/dev/null || launchctl load "$DST"; echo "installed + started";;
  start)   launchctl bootstrap "gui/$(id -u)" "$DST" 2>/dev/null || launchctl load "$DST"; echo "started";;
  stop)    launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || launchctl unload "$DST"; echo "stopped (clean flush on SIGTERM)";;
  status)  launchctl print "gui/$(id -u)/$LABEL" 2>/dev/null | grep -E "state|pid|last exit" || echo "not loaded"; tail -n 3 "$(dirname "$SRC")/../logs/recorder.log" 2>/dev/null;;
  *) echo "usage: $0 install|start|stop|status [label]"; exit 1;;
esac
