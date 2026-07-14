VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)

PLATFORMS := darwin/arm64 darwin/amd64 linux/arm64 linux/amd64

.PHONY: help build install test race test-all vet fmt fmt-check lint cross clean check

help: ## Show this help message
	@echo "loco - available make targets:"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the loco binary into bin/loco
	go build -ldflags "$(LDFLAGS)" -o bin/loco ./cmd/loco

install: ## Install loco via go install
	go install -ldflags "$(LDFLAGS)" ./cmd/loco

test: ## Run the fast test suite
	go test ./...

race: ## Run the short test suite with the race detector
	go test -race -short ./...

test-all: ## Run the full test suite, including slow tests
	go test ./...

vet: ## Run go vet
	go vet ./...

fmt: ## Reformat all Go source files
	gofmt -w .

fmt-check: ## Fail if any Go source file is not gofmt-formatted
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		echo "The following files are not gofmt-formatted:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

lint: ## Run golangci-lint (requires golangci-lint to be installed)
	golangci-lint run

cross: ## Cross-compile for darwin/linux x arm64/amd64 into dist/
	@mkdir -p dist
	@for platform in $(PLATFORMS); do \
		os=$$(echo $$platform | cut -d/ -f1); \
		arch=$$(echo $$platform | cut -d/ -f2); \
		echo "Building $$os/$$arch..."; \
		GOOS=$$os GOARCH=$$arch go build -ldflags "$(LDFLAGS)" -o dist/loco-$$os-$$arch ./cmd/loco || exit 1; \
	done

clean: ## Remove build artifacts
	rm -rf bin dist

check: fmt-check vet test race ## Pre-PR gate: fmt-check, vet, test, race
