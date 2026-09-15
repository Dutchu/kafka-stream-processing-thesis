#!/bin/bash
set -uo pipefail

INBOX=/opt/thesis-deploy/inbox
STATE=/opt/thesis-deploy
JAR_TARGET=/opt/thesis-dashboard/dashboard/target/thesis-dashboard-2.0.0.jar
SERVICE=thesis-dashboard
HEALTH=http://localhost:8080/api/health

log() {
  echo "thesis-deploy-agent: $*" | /usr/bin/systemd-cat -t thesis-deploy 2>/dev/null \
    || echo "thesis-deploy-agent: $*"
}

wanted="$(tr -d ' \t\r\n' < "$INBOX/version.txt")"
current="$(cat "$STATE/current-sha" 2>/dev/null || echo none)"
if [[ "$wanted" == "$current" ]]; then
  log "already at $wanted, nothing to do"
  exit 0
fi
if [[ ! -f "$INBOX/thesis-dashboard-2.0.0.jar" ]]; then
  log "jar missing in inbox for $wanted"
  echo "SHA=$wanted RESULT=FAIL TS=$(date -u +%FT%TZ)" > "$STATE/status"
  exit 1
fi
sleep 3

[[ -f "$JAR_TARGET" ]] && cp -f "$JAR_TARGET" "$JAR_TARGET.prev"
systemctl stop "$SERVICE" || true
pkill -f 'thesis-dashboard.*[.]jar' || true
sleep 3
cp -f "$INBOX/thesis-dashboard-2.0.0.jar" "$JAR_TARGET"
systemctl start "$SERVICE"

ok=0
for _ in $(seq 1 12); do
  sleep 5
  if out="$(curl -sf --max-time 10 "$HEALTH" 2>/dev/null)" \
    && printf '%s' "$out" | grep -q '"status":"ok"'; then
    ok=1
    break
  fi
done

if [[ "$ok" -eq 1 ]]; then
  echo "$wanted" > "$STATE/current-sha"
  echo "SHA=$wanted RESULT=OK TS=$(date -u +%FT%TZ)" > "$STATE/status"
  log "deployed $wanted OK"
  exit 0
fi

log "health failed for $wanted — rolling back"
if [[ -f "$JAR_TARGET.prev" ]]; then
  cp -f "$JAR_TARGET.prev" "$JAR_TARGET"
  systemctl restart "$SERVICE" || true
fi
echo "SHA=$wanted RESULT=ROLLED-BACK TS=$(date -u +%FT%TZ)" > "$STATE/status"
exit 1
