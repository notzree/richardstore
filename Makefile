.PHONY: build run test

build:
	@go build -v -o $$(pwd)/bin/rs

run: build
	@$$(pwd)/bin/rs

test:
	go test ./... 