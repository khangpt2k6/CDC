# CDC platform task runner.
#
# Common targets used locally and in CI. `generate` wraps buf codegen (Issue 0.2).

GO       ?= go
BUF      ?= buf
BIN_DIR  ?= bin

.PHONY: all build test lint generate tidy clean

all: build test lint

## build: compile all packages and the worker binary
build:
	$(GO) build ./...
	$(GO) build -o $(BIN_DIR)/worker ./cmd/worker

## test: run the unit test suite
test:
	$(GO) test ./...

## lint: run go vet and golangci-lint
lint:
	$(GO) vet ./...
	golangci-lint run

## generate: regenerate protobuf Go bindings from proto/
generate:
	$(BUF) generate

## tidy: tidy and verify the module graph
tidy:
	$(GO) mod tidy

## clean: remove build artifacts
clean:
	rm -rf $(BIN_DIR)
