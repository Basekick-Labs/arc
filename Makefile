.PHONY: help build build-fips test test-fips run clean install deps fmt lint fips-check

# Variables
BINARY_NAME=arc
GO=go
GOFLAGS=-v -tags=duckdb_arrow
MAIN_PATH=./cmd/arc

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

fips-check: ## Verify the fips build links no non-FIPS crypto (x/crypto/bcrypt, x/crypto/hkdf)
	@echo "Checking fips build import graph for non-approved crypto..."
	@if $(GO) list $(FIPS_GOFLAGS) -deps $(MAIN_PATH) 2>/dev/null | grep -E 'golang.org/x/crypto/(bcrypt|hkdf)'; then \
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
