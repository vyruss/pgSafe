#!/usr/bin/env bash
# Wrapper: brings up the sshtest fixture in the background, generates an
# age identity + YAML config, records the operator-flow demo with
# asciinema, then tears the fixture down.
#
# Outputs:
#   test/demo/pgsafe-speed-comparison.cast  — the asciinema recording
#   test/demo/pgsafe-speed-comparison.txt   — plain-text transcript
set -euo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
DEMO_DIR="$REPO/test/demo"
CAST="$DEMO_DIR/pgsafe-speed-comparison.cast"
TXT="$DEMO_DIR/pgsafe-speed-comparison.txt"

cleanup() {
  if [[ -n "${HOLD_PID:-}" ]]; then
    kill "$HOLD_PID" 2>/dev/null || true
    wait "$HOLD_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

cd "$REPO"

echo "[record] building host pgsafe binary"
make build >/dev/null

echo "[record] generating age identity"
go run "$DEMO_DIR/genage" > /tmp/pgsafe-demo-age.txt
grep -E '^AGE-SECRET-KEY' /tmp/pgsafe-demo-age.txt > /tmp/pgsafe-demo-age.key
chmod 600 /tmp/pgsafe-demo-age.key

echo "[record] starting sshtest fixture (this builds the PG+sshd image, ~30s)"
rm -f /tmp/pgsafe-demo-env.sh
PGSAFE_DEMO_HOLD_SECONDS=1800 \
  go test -tags=demo_fixture -run TestHoldFixture -v -timeout=2400s ./test/demo/ \
  >/tmp/pgsafe-demo-fixture.log 2>&1 &
HOLD_PID=$!

# Wait up to 5 minutes for the env file to appear.
for _ in $(seq 1 300); do
  [[ -f /tmp/pgsafe-demo-env.sh ]] && break
  sleep 1
done
if [[ ! -f /tmp/pgsafe-demo-env.sh ]]; then
  echo "[record] fixture never wrote env file; tail of log:"
  tail -40 /tmp/pgsafe-demo-fixture.log >&2
  exit 1
fi

source /tmp/pgsafe-demo-env.sh
RECIPIENT=$(grep -E '^age1' /tmp/pgsafe-demo-age.txt)
mkdir -p /tmp/pgsafe-demo-config

# Operator-style ssh_config: one Host entry, all the per-target knobs in
# one place. The recorded backup command then needs only a single
# `--ssh-extra-arg=-F=...` to consume it.
cat >/tmp/pgsafe-demo-config/ssh_config <<EOF
Host pg-host
    HostName ${PGSAFE_DEMO_SSH_HOST}
    Port ${PGSAFE_DEMO_SSH_PORT}
    User ${PGSAFE_DEMO_SSH_USER}
    IdentityFile ${PGSAFE_DEMO_SSH_KEY}
    UserKnownHostsFile ${PGSAFE_DEMO_SSH_KNOWN_HOSTS}
    StrictHostKeyChecking yes
    BatchMode yes
EOF
cat >/tmp/pgsafe-demo-config/pgsafe.yaml <<EOF
server: demo
pg:
  conn_string: "${PGSAFE_DEMO_SUPER_DSN}"
  version: ${PGSAFE_DEMO_PG_VERSION}
storage:
  type: posix
  path: ${PGSAFE_DEMO_HOST_STORAGE}
compression:
  codec: gzip
  level: 0
encryption:
  recipients:
    - "${RECIPIENT}"
log:
  format: text
  level: info
EOF

echo "[record] fixture ready; recording demo to $CAST"
export TERM="${TERM:-xterm-256color}"
asciinema rec "$CAST" \
  --overwrite \
  --env "PGSAFE_DEMO_SSH_TARGET,PGSAFE_DEMO_SSH_PORT,PGSAFE_DEMO_HOST_STORAGE,PGSAFE_DEMO_REMOTE_BIN" \
  --cols 100 --rows 30 \
  --title "pgSafe — PG-native vs pgSafe-mode (4 workers), 10 M-row backup" \
  -c "bash $DEMO_DIR/demo-run.sh"

asciinema cat "$CAST" > "$TXT"

echo "[record] cast: $CAST"
echo "[record] txt:  $TXT"
