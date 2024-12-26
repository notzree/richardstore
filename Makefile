.PHONY: build run test
.PHONY: proto

build:
	@go build -v -o $$(pwd)/bin/rs

run: build
	@$$(pwd)/bin/rs

test:
	go test ./... 

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/schema.proto

.PHONY: proto
