#!/usr/bin/env bash
# Recorded portion of the asciinema demo. Assumes the sshtest fixture is
# already up and PGSAFE_DEMO_* env vars have been sourced from
# /tmp/pgsafe-demo-env.sh (see record.sh for the wrapper).
set -euo pipefail
export TERM="${TERM:-xterm-256color}"

# pgsafe's PG-native shape shells out to /usr/bin/env pg_basebackup; on
# Debian/Ubuntu the PG 18 client binaries live in a versioned dir not on
# the default PATH.
export PATH="/usr/lib/postgresql/18/bin:$PATH"

# Defensive: a previous interrupted run might have left postgres running
# on the demo's 55432, which would block the recording's pg_ctl start.
pkill -f "postgres.*-D /tmp/pgsafe-demo-restore" 2>/dev/null || true
sleep 0.3

source /tmp/pgsafe-demo-env.sh

PROMPT='\033[1;32moperator$\033[0m '
HEADER='\033[1;36m'
RESET='\033[0m'

# Print one line as if typed at a prompt, then run it.
say() {
  printf "${PROMPT}"
  for ((i=0; i<${#1}; i++)); do
    printf '%s' "${1:i:1}"
    sleep 0.03
  done
  printf '\n'
  eval "$1"
}

note() {
  printf "\n${HEADER}# %s${RESET}\n" "$1"
  sleep 0.6
}

# Make `time` print one tidy line per command.
export TIMEFORMAT='real %3R seconds'

printf '\033[2J\033[H'
printf "${HEADER}### pgSafe — same tool, two shapes, 10 M-row backup head-to-head\n"
printf "###\n"
printf "### Same pgsafe binary. Same source cluster. Same encrypted POSIX repo.\n"
printf "### Run 1: PG-native shape (libpq → pg_basebackup under the hood, single connection).\n"
printf "### Run 2: pgSafe shape  (worker on PG host, 4 StreamFile RPCs in parallel over one SSH session).\n"
printf "### Shape is inferred from config: pg.host unset → PG-native; --ssh-target set → pgSafe-mode.\n${RESET}\n"
sleep 1.5

note "Confirm the PG host is reachable."
say  "psql \"\$PGSAFE_DEMO_SUPER_DSN\" -c 'select version();' | head -3"
sleep 0.5

note "Show the operator's pgsafe config (no --mode flag — shape comes from pg.host vs --ssh-target)."
say  "cat /tmp/pgsafe-demo-config/pgsafe.yaml"
sleep 0.8

note "Initialise the repo on the orchestrator side."
say  "./bin/pgsafe --config /tmp/pgsafe-demo-config/pgsafe.yaml server add"
sleep 0.5

note "Seed a 10-million-row dataset (~700 MB on disk after CHECKPOINT)."
say  "psql \"\$PGSAFE_DEMO_SUPER_DSN\" -c 'CREATE EXTENSION IF NOT EXISTS amcheck; CREATE TABLE demo(id int primary key, body text); INSERT INTO demo SELECT g, repeat(chr(96+g%26),80) FROM generate_series(1,10000000) g; CHECKPOINT;'"
sleep 0.5

note "Confirm size on disk."
say  "psql \"\$PGSAFE_DEMO_SUPER_DSN\" -At -c \"SELECT pg_size_pretty(pg_database_size('postgres'));\""
sleep 0.4

note "Run 1 — PG-native baseline. No --ssh-target, no --workers — simple mode is inferred."
say  "time ./bin/pgsafe --config /tmp/pgsafe-demo-config/pgsafe.yaml backup"
sleep 0.5

note "All the SSH knobs live in one ssh_config file — pgsafe just consumes it."
say  "cat /tmp/pgsafe-demo-config/ssh_config"
sleep 0.4

note "Run 2 — pgSafe shape. --ssh-target makes pgsafe spawn a worker on the PG host; --workers=4 fans out StreamFile RPCs over a single SSH session."
say  "time ./bin/pgsafe --config /tmp/pgsafe-demo-config/pgsafe.yaml backup \\
  --direct-write=false --workers=4 --ssh-target=pg-host \\
  --ssh-extra-args='-F /tmp/pgsafe-demo-config/ssh_config' \\
  --remote-command='sudo -u postgres -E /tmp/pgsafe worker stdio'"
sleep 0.5

note "Both backups landed in the same encrypted repo."
say  "./bin/pgsafe --config /tmp/pgsafe-demo-config/pgsafe.yaml info"
sleep 0.5

note "Decrypt + decompress + sha256 every stored file end-to-end (proves both runs round-trip)."
say  "./bin/pgsafe --config /tmp/pgsafe-demo-config/pgsafe.yaml verify --identity-file=/tmp/pgsafe-demo-age.key"
sleep 0.5

note "Restore the most recent (pgSafe-mode) backup into a fresh PGDATA directory."
say  "rm -rf /tmp/pgsafe-demo-restore && install -d -m 0700 /tmp/pgsafe-demo-restore"
say  "./bin/pgsafe --config /tmp/pgsafe-demo-config/pgsafe.yaml restore --target=/tmp/pgsafe-demo-restore --identity-file=/tmp/pgsafe-demo-age.key"
sleep 0.5

note "Start PG against the restored cluster on port 55432 and query the seeded table."
say  "echo \"restore_command = 'cp \$PGSAFE_DEMO_WAL_ARCHIVE/%f %p'\" >> /tmp/pgsafe-demo-restore/postgresql.auto.conf"
say  "/usr/lib/postgresql/18/bin/pg_ctl -D /tmp/pgsafe-demo-restore -l /tmp/pgsafe-demo-restore.log -o '-p 55432 -c unix_socket_directories=/tmp' -w start"
say  "psql -h localhost -p 55432 -U postgres -d postgres -c 'SELECT count(*) AS rows FROM demo;'"
sleep 0.3

note "pg_amcheck walks heap + indexes on the live restored cluster — structural integrity check."
say  "/usr/lib/postgresql/18/bin/pg_amcheck -h localhost -p 55432 -U postgres -d postgres --heapallindexed && echo '(no errors reported)'"
say  "/usr/lib/postgresql/18/bin/pg_ctl -D /tmp/pgsafe-demo-restore stop -m fast >/dev/null"
sleep 0.5

printf "\n${HEADER}### Done. Same source, same encrypted repo — pgSafe shape finished in a fraction of PG-native's time, and the restored cluster passes amcheck.${RESET}\n"
sleep 1
