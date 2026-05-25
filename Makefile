.PHONY: fmt test test-unit test-integration test-cloud test-e2e test-faults build release ci

fmt:
	gofmt -w .
	go tool goimports -w .

test: test-unit test-integration test-cloud

test-unit:
	go test -race -count=1 -short ./...

test-integration:
	go test -race -count=1 -tags=integration ./...

test-cloud:
	go test -race -count=1 -tags=integration_cloud ./internal/storage/cloudtest/...

test-e2e:
	go test -race -count=1 -tags=e2e ./test/e2e/...

test-faults:
	go test -race -count=1 -tags=faults ./test/faults/...

build:
	mkdir -p bin
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/pgsafe ./cmd/pgsafe

release:
	mkdir -p dist
	@for target in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64; do \
		GOOS=$${target%/*} GOARCH=$${target#*/} CGO_ENABLED=0 \
			go build -ldflags="-s -w" -o dist/pgsafe-$${target%/*}-$${target#*/} ./cmd/pgsafe; \
	done

ci:
	./run-ci-local.sh
