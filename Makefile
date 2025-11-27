.PHONY: help build test run clean install deps fmt lint

# Variables
BINARY_NAME=arc
GO=go
GOFLAGS=-v
GOTAGS=-tags=duckdb_arrow
MAIN_PATH=./cmd/arc

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

build: ## Build the binary (with Arrow support)
	$(GO) build $(GOFLAGS) $(GOTAGS) -o $(BINARY_NAME) $(MAIN_PATH)

build-minimal: ## Build without Arrow support (smaller binary)
	$(GO) build $(GOFLAGS) -o $(BINARY_NAME) $(MAIN_PATH)

run: ## Run Arc directly (without building)
	$(GO) run $(GOTAGS) $(MAIN_PATH)

test: ## Run all tests
	$(GO) test $(GOTAGS) -v -race -coverprofile=coverage.out ./...

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
