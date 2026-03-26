.PHONY: all build test test-int test-chaos test-load test-reliability lint proto docker-build up down clean deps

# Default target
all: build test

# Build all binaries
build:
	go build ./...
	go build -o bin/coordinator ./cmd/coordinator
	go build -o bin/node ./cmd/node

# Run unit tests (fast, no network)
test:
	go test ./... -count=1 -timeout=30s

# Run integration tests (starts real servers)
test-int:
	go test ./test/integration/... -count=1 -timeout=60s -v -tags=integration

# Run all tests
test-all: test test-int

# Run chaos tests only (subset of reliability)
test-chaos:
	go test ./test/reliability/... -count=1 -timeout=120s -v -tags=reliability -run TestChaos

# Run load tests only (subset of reliability)
test-load:
	go test ./test/reliability/... -count=1 -timeout=120s -v -tags=reliability -run TestLoad

# Run all reliability tests (chaos + load + monkey)
test-reliability:
	go test ./test/reliability/... -count=1 -timeout=300s -v -tags=reliability

# Run linter
lint:
	golangci-lint run ./...

# Generate protobuf code (requires buf: brew install bufbuild/buf/buf)
proto:
	buf generate

# Build Docker images
docker-build:
	docker build -f docker/Dockerfile.coordinator -t doki-coordinator:latest .
	docker build -f docker/Dockerfile.node -t doki-node:latest .

# Start the cluster
up:
	docker compose up -d

# Stop the cluster
down:
	docker compose down

# Tidy dependencies
deps:
	go mod tidy

# Remove build artifacts
clean:
	go clean ./...
	rm -rf gen/

# Run smoke test against a running cluster
smoke:
	./scripts/smoke_test.sh
