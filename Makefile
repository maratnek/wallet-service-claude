.PHONY: help build up down test test-verbose test-cover test-race logs logs-server logs-collector watch-collector open-jaeger tidy clean proto proto-gen proto-clean kafka-init

# ── Config ────────────────────────────────────────────────────────────────────
BIN_DIR      := bin
SERVER_BIN   := $(BIN_DIR)/wallet-server
WORKER_BIN   := $(BIN_DIR)/wallet-worker
CLIENT_BIN   := $(BIN_DIR)/wallet-client

# Build for Linux/amd64 — matches alpine container
# Change to GOARCH=arm64 if you're on Apple Silicon running arm64 containers
GOOS         := linux
GOARCH       := amd64

# Colors
GREEN  := \033[0;32m
YELLOW := \033[0;33m
RESET  := \033[0m

# Определяем архитектуру
ARCH := $(shell uname -m)
ifeq ($(ARCH),arm64)
    GOARCH := arm64
else ifeq ($(ARCH),aarch64)
    GOARCH := arm64
else
    GOARCH := amd64
endif

# ── Help ──────────────────────────────────────────────────────────────────────

help: ## Show all commands
	@echo ""
	@echo "  Wallet Service — gRPC + OpenTelemetry"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(RESET) %s\n", $$1, $$2}'
	@echo ""
	@echo "  Typical flow:"
	@echo "    make tidy    — download dependencies"
	@echo "    make test    — run unit tests"
	@echo "    make build   — compile binaries to bin/"
	@echo "    make up      — build images and start all containers"
	@echo ""

# ── Dependencies ──────────────────────────────────────────────────────────────

tidy: ## Download and tidy go modules
	@echo "$(YELLOW)Tidying modules...$(RESET)"
	go mod tidy
	@echo "$(GREEN)Done$(RESET)"

proto-clean: ## Remove generated protobuf code files
	@rm -f proto/wallet.pb.go proto/wallet_grpc.pb.go
	@rm -rf proto/proto
	@echo "$(YELLOW)Cleaned proto generated files$(RESET)"

proto-gen: proto-clean ## Generate protobuf Go code from proto definitions
	@echo "$(YELLOW)Generating protobuf code into proto/...$(RESET)"
	protoc --go_out=paths=source_relative:. \
		--go-grpc_out=paths=source_relative:. \
		proto/wallet.proto \
       proto/messages.proto
	@echo "$(GREEN)Generated proto code$(RESET)"

proto: proto-gen ## Alias for proto-gen

# ── Tests ─────────────────────────────────────────────────────────────────────

test: ## Run unit tests (no Docker, no network)
	@echo "$(YELLOW)Running unit tests...$(RESET)"
	go test ./internal/... -count=1
	@echo "$(GREEN)All tests passed$(RESET)"

test-verbose: ## Run tests with full output per test
	@echo "$(YELLOW)Running unit tests (verbose)...$(RESET)"
	go test ./internal/... -v -count=1

test-cover: ## Run tests and show coverage per function
	@mkdir -p $(BIN_DIR)
	@echo "$(YELLOW)Running tests with coverage...$(RESET)"
	go test ./internal/... -count=1 -coverprofile=$(BIN_DIR)/coverage.out
	@go tool cover -func=$(BIN_DIR)/coverage.out
	@echo ""
	@echo "  $(GREEN)HTML report:$(RESET) go tool cover -html=$(BIN_DIR)/coverage.out"

test-race: ## Run tests with race detector (important for concurrent repo)
	@echo "$(YELLOW)Running tests with race detector...$(RESET)"
	go test ./internal/... -race -count=1

# ── Build ─────────────────────────────────────────────────────────────────────

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

build: proto-gen $(BIN_DIR) ## Compile binaries to bin/ (linux/amd64 for Docker)
	@echo "$(YELLOW)Building server ($(GOOS)/$(GOARCH))...$(RESET)"
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build \
		-o $(SERVER_BIN) \
		./cmd/server
	@echo "$(YELLOW)Building worker ($(GOOS)/$(GOARCH))...$(RESET)"
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build \
		-o $(WORKER_BIN) \
		./cmd/worker
	@echo "$(YELLOW)Building client ($(GOOS)/$(GOARCH))...$(RESET)"
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build \
		-o $(CLIENT_BIN) \
		./client
	@echo "$(GREEN)Built:$(RESET)"
	@ls -lh $(SERVER_BIN) $(WORKER_BIN) $(CLIENT_BIN)

build-local: $(BIN_DIR) ## Compile binaries for your host OS (run without Docker)
	@echo "$(YELLOW)Building for host OS...$(RESET)"
	go build -o $(BIN_DIR)/server-local ./cmd/server
	go build -o $(BIN_DIR)/client-local ./client
	@echo "$(GREEN)Built: $(BIN_DIR)/server-local, $(BIN_DIR)/client-local$(RESET)"

# ── Docker ────────────────────────────────────────────────────────────────────

docker-build: build ## Build Docker images using pre-compiled binaries
# 	@echo "$(YELLOW)Building Docker images...$(RESET)"
# 	docker build -f Dockerfile.server -t wallet-service:local .
# 	docker build -f Dockerfile.client -t wallet-client:local .
# 	@echo "$(GREEN)Images ready: wallet-service:local, wallet-client:local$(RESET)"

kafka-init: ## Pre-create Kafka topics + __consumer_offsets to avoid first-message auto-create latency
	@echo "$(YELLOW)Waiting for Kafka to accept connections...$(RESET)"
	@for i in $$(seq 1 30); do \
		docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break; \
		sleep 1; \
	done
	@echo "$(YELLOW)Pre-creating topics (wallet.commands, wallet.results, __consumer_offsets)...$(RESET)"
	@docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic wallet.commands --partitions 1 --replication-factor 1
	@docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic wallet.results --partitions 1 --replication-factor 1
	@docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic __consumer_offsets --partitions 50 --replication-factor 1 \
		--config compression.type=producer --config cleanup.policy=compact --config segment.bytes=104857600
	@echo "$(GREEN)Kafka warmed up$(RESET)"

up: docker-build ## Build binaries + images then start all containers
	@echo "$(YELLOW)Starting all services...$(RESET)"
	@docker compose down --remove-orphans 2>/dev/null || true
	docker compose up -d kafka jaeger otel-collector
	@$(MAKE) kafka-init
	docker compose up -d wallet-service wallet-worker wallet-client
	@echo ""
	@echo "  $(GREEN)Jaeger UI:$(RESET) http://localhost:16686"

up-detach: docker-build ## Start everything in background
	docker compose up -d
	@echo "$(GREEN)Jaeger UI:$(RESET) http://localhost:16686"
	@echo "Follow logs: make logs"

down: ## Stop and remove all containers
	docker compose down

# ── Logs ─────────────────────────────────────────────────────────────────────

logs: ## Tail logs from all services
	docker compose logs -f

logs-server: ## Tail wallet-service logs only
	docker compose logs -f wallet-service

logs-collector: ## Tail otel-collector logs (shows span batches)
	docker compose logs -f otel-collector

watch-collector: ## Watch otel-collector logs live (updates every 2s)
	@command -v watch >/dev/null 2>&1 \
		&& watch -n 2 "docker compose logs --tail=10 otel-collector" \
		|| while true; do docker compose logs --tail=10 otel-collector; sleep 2; done

open-jaeger: ## Open Jaeger UI in browser
	xdg-open http://localhost:16686 &

# ── Clean ─────────────────────────────────────────────────────────────────────

clean: proto-clean ## Remove bin/ and stop containers
	docker compose down 2>/dev/null || true
	rm -rf $(BIN_DIR)
	@echo "$(GREEN)Cleaned$(RESET)"

# watch
watch: ## Watch docker ps every 2s — shows container status live
	@command -v watch >/dev/null 2>&1 \
		&& watch -n 2 "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' | grep -E 'wallet|jaeger|otel|NAMES'" \
		|| while true; do docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' | grep -E 'wallet|jaeger|otel|NAMES'; sleep 2; done