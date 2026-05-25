#!/usr/bin/env bash
# Canonical pre-commit gate for pgSafe.
# This script is the single source of truth; GitHub Actions invokes the
# same script step-for-step.
set -euo pipefail

cd "$(dirname "$0")"

LOG="ci-$(date -u +%Y%m%dT%H%M%SZ).log"
exec > >(tee "$LOG") 2>&1

step() {
	printf '\n=== %s ===\n' "$1"
}

step "1/14 gofmt"
fmt_out=$(gofmt -l . | grep -v '^vendor/' || true)
if [ -n "$fmt_out" ]; then
	echo "files need gofmt:"; echo "$fmt_out"; exit 1
fi

step "2/14 goimports"
imp_out=$(go tool goimports -l . | grep -v '^vendor/' || true)
if [ -n "$imp_out" ]; then
	echo "files need goimports:"; echo "$imp_out"; exit 1
fi

step "3/14 golangci-lint (includes go vet via govet linter)"
# .golangci.yml enables `govet` explicitly so we don't run `go vet ./...`
# separately — a guard against the two configs drifting. If you remove
# govet from .golangci.yml, restore a `go vet` step here.
if ! grep -qE '^[[:space:]]*-[[:space:]]*govet[[:space:]]*$' .golangci.yml; then
	echo "FAIL: govet must be enabled in .golangci.yml (or add a separate go vet step)"
	exit 1
fi
go tool golangci-lint run ./...

step "4/14 unit tests"
go test -race -count=1 -short ./...

step "5/14 build all release targets"
mkdir -p bin
for target in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64; do
	GOOS=${target%/*} GOARCH=${target#*/} CGO_ENABLED=0 \
		go build -ldflags="-s -w" -o "bin/pgsafe-${target%/*}-${target#*/}" ./cmd/pgsafe
done

step "6/14 integration tests"
if [ "${PGSAFE_SKIP_INTEGRATION:-0}" = "1" ]; then
	echo "(skipped via PGSAFE_SKIP_INTEGRATION=1)"
else
	go test -race -count=1 -tags=integration ./...
fi

step "7/14 cloud-emulator integration tests"
if [ "${PGSAFE_SKIP_CLOUD:-0}" = "1" ]; then
	echo "(skipped via PGSAFE_SKIP_CLOUD=1)"
else
	# All packages tagged integration_cloud — covers the cloudtest fixtures
	# AND the per-driver tests under internal/storage/{s3,azure,gcs,sftp}.
	go test -race -count=1 -tags=integration_cloud ./...
fi

step "8/14 multi-host (pgSafe-worker) integration tests"
if [ "${PGSAFE_SKIP_HYBRID:-0}" = "1" ]; then
	echo "(skipped via PGSAFE_SKIP_HYBRID=1)"
else
	go test -race -count=1 -tags=integration_hybrid ./...
fi

step "9/14 fault-injection tests"
if [ ! -d test/faults ]; then
	echo "(no fault tests yet — test/faults/ does not exist)"
else
	# faults,integration_hybrid covers both the standalone fault tests
	# (Invariant #6, #7) and the cross-host Tenet-3 cred-disk test that
	# requires the sshtest fixture.
	go test -race -count=1 -tags=faults,integration_hybrid ./test/faults/...
fi

step "10/14 e2e tests"
if [ "${PGSAFE_SKIP_E2E:-0}" = "1" ]; then
	echo "(skipped via PGSAFE_SKIP_E2E=1)"
elif [ ! -d test/e2e ]; then
	echo "(no e2e tests yet — test/e2e/ does not exist)"
else
	go test -race -count=1 -tags=e2e ./test/e2e/...
fi

step "11/14 named invariant tests"
if [ ! -d test/invariants ]; then
	echo "(no invariant tests yet — test/invariants/ does not exist)"
else
	go test -race -count=1 -tags=invariants ./test/invariants/...
fi

step "12/14 matrix (PG 18 by default; PGSAFE_MATRIX_PG=N for one version, =all for all six)"
if [ "${PGSAFE_SKIP_MATRIX:-0}" = "1" ]; then
	echo "(skipped via PGSAFE_SKIP_MATRIX=1)"
elif [ ! -d test/matrix ]; then
	echo "(no matrix tests yet — test/matrix/ does not exist)"
else
	# No -race here: the matrix runs many PG containers + cloud emulators
	# concurrently, and the race detector triples memory pressure for
	# little signal (the orchestrator code is already race-tested in
	# step 4 unit tests). Re-introduce -race here if a matrix-only race
	# shows up.
	go test -count=1 -tags=matrix -timeout=45m ./test/matrix/...
fi

step "13/14 perf benchmarks (vs. pgbackrest)"
if [ "${PGSAFE_RUN_PERF:-0}" != "1" ]; then
	echo "(skipped — set PGSAFE_RUN_PERF=1 to run)"
elif [ ! -d test/perf ]; then
	echo "(no perf harness yet — test/perf/ does not exist)"
else
	go test -count=1 -tags=perf -timeout=4h ./test/perf/...
fi

step "14/14 stale log purge"
ls -1t ci-*.log 2>/dev/null | tail -n +2 | xargs -r rm -f

echo
echo "OK: run-ci-local.sh exit 0"
