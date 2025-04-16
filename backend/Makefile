.PHONY: run build clean test

# Default target
all: build

# Build the application
build:
	go build -o bin/server cmd/server/main.go

# Run the application
run:
	go run cmd/server/main.go

# Clean build artifacts
clean:
	rm -rf bin/
	rm -rf uploads/

# Install dependencies
deps:
	go mod tidy

# Create necessary directories
setup:
	mkdir -p bin uploads

# Run tests
test:
	go test ./...

# Default target
.DEFAULT_GOAL := run

