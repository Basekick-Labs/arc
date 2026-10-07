.PHONY: help build build-fips build-arcx test test-fips test-arcx run clean install deps fmt lint fips-check arcx-lib

# Variables
BINARY_NAME=arc
GO=go
GOFLAGS=-v -tags=duckdb_arrow
MAIN_PATH=./cmd/arc

# arcx engine (standalone Rust query engine, in-process via cgo/FFI). Opt-in: the
# default build stays pure-Go(+duckdb) and needs no Rust toolchain. `build-arcx`
# compiles libarcx.a from the sibling arcx repo, then links it via the arcx_engine
# build tag. ARCX_DIR points at the arcx checkout (sibling by default). See
# arcx/docs/2026-07-05-ffi-bridge-design.md.
ARCX_DIR ?= ../arcx
ARCX_GOFLAGS=-v -tags=duckdb_arrow,arcx_engine

# FIPS build variant. Same source/commit/version as the standard build — only
# the build tag and the GOFIPS140 module selection differ. GOFIPS140=v1.0.0 is
# the CMVP-certified Go Cryptographic Module snapshot (see
# $(shell go env GOROOT)/lib/fips140/certified.txt). The fips tag enables
# fail-closed legacy-token verification and bakes in GODEBUG=fips140=only.
FIPS_BINARY_NAME=arc-fips
FIPS_GOFLAGS=-v -tags=duckdb_arrow,fips
GOFIPS140_VERSION=v1.0.0

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

deps: ## Download Go dependencies
	$(GO) mod download
	$(GO) mod verify

install: ## Install dependencies (alias for deps)
	@make deps

build: ## Build the binary
	$(GO) build $(GOFLAGS) -o $(BINARY_NAME) $(MAIN_PATH)

build-fips: ## Build the FIPS 140-3 variant (arc-fips) against the certified Go module
	GOFIPS140=$(GOFIPS140_VERSION) CGO_ENABLED=1 $(GO) build $(FIPS_GOFLAGS) -o $(FIPS_BINARY_NAME) $(MAIN_PATH)

# arcx-lib cds into $(ARCX_DIR) instead of passing --manifest-path: cargo discovers
# .cargo/config.toml from the CWD upward, never from the manifest's directory, so
# building it from here silently dropped arcx's MACOSX_DEPLOYMENT_TARGET pin (arcx
# #55) and stamped the C objects (zstd, mimalloc) with minos = the HOST SDK.
# Measured 2026-10-02: --manifest-path from here -> minos 27.0; cd first -> 26.0.
# The former ships an `arc` that advertises macOS 26 support while embedding objects
# that demand 27. (The ~22 "built for newer macOS version" ld warnings are a
# DIFFERENT thing and survive either way — they track the objects' `sdk` field, i.e.
# whichever SDK is installed; only an older -isysroot silences those. Don't read
# them as this pin failing.)
arcx-lib: ## Build the arcx engine static lib (libarcx.a) from $(ARCX_DIR)
	@command -v cargo >/dev/null || { echo "ERROR: cargo (Rust) required for the arcx build; install rustup"; exit 1; }
	@test -d "$(ARCX_DIR)" || { echo "ERROR: arcx repo not found at ARCX_DIR=$(ARCX_DIR)"; exit 1; }
	cd $(ARCX_DIR) && cargo build --release

build-arcx: arcx-lib ## Build arc with the arcx engine linked in (in-process FFI)
	CGO_ENABLED=1 $(GO) build $(ARCX_GOFLAGS) -o $(BINARY_NAME) $(MAIN_PATH)

test-arcx: arcx-lib ## Run tests with the arcx engine linked in (exercises the FFI bridge)
	CGO_ENABLED=1 $(GO) test $(ARCX_GOFLAGS) ./...

fips-check: ## Verify the fips build links no non-FIPS crypto (x/crypto/bcrypt, x/crypto/hkdf)
	@echo "Checking fips build import graph for non-approved crypto..."
	@out=$$($(GO) list $(FIPS_GOFLAGS) -deps $(MAIN_PATH) 2>&1); \
	if [ $$? -ne 0 ]; then \
		echo "ERROR: go list failed (fips build does not compile?):"; echo "$$out"; exit 1; \
	fi; \
	if echo "$$out" | grep -E 'golang.org/x/crypto/(bcrypt|hkdf)'; then \
		echo "ERROR: fips build pulls in non-FIPS crypto above"; exit 1; \
	else \
		echo "OK: no x/crypto/bcrypt or x/crypto/hkdf in the fips build"; \
	fi

run: ## Run Arc directly (without building)
	$(GO) run $(GOFLAGS) $(MAIN_PATH)

test: ## Run all tests
	$(GO) test $(GOFLAGS) -race -coverprofile=coverage.out ./...

test-fips: ## Run all tests with the fips build tag (exercises fail-closed paths)
	$(GO) test $(FIPS_GOFLAGS) ./...

test-coverage: test ## Run tests with coverage report
	$(GO) tool cover -html=coverage.out

bench: ## Run benchmarks
	$(GO) test -bench=. -benchmem ./...

fmt: ## Format Go code
	$(GO) fmt ./...
	gofmt -s -w .

lint: ## Run linter (requires golangci-lint)
	golangci-lint run

clean: ## Clean build artifacts
	rm -f $(BINARY_NAME)
	rm -f $(FIPS_BINARY_NAME)
	rm -f coverage.out
	rm -rf ./data/arc/*

dev: ## Run in development mode with hot reload (requires air)
	air

docker-build: ## Build Docker image
	docker build -t arc:latest .

docker-run: ## Run Docker container
	docker run -p 8000:8000 arc:latest

# Development helpers
watch-test: ## Watch and run tests on file changes (requires entr)
	find . -name '*.go' | entr -c make test

.DEFAULT_GOAL := help
