.PHONY: build run test proto build-namenode build-datanode run-namenode run-datanode
.PHONY: proto

# Binary output paths
NAMENODE_BIN = $$(pwd)/bin/namenode
DATANODE_BIN = $$(pwd)/bin/datanode

build: build-namenode build-datanode

build-namenode:
	@go build -v -o $(NAMENODE_BIN) ./cmd/namenode

build-datanode:
	@go build -v -o $(DATANODE_BIN) ./cmd/datanode

run-namenode: build-namenode
	@chmod +x $(NAMENODE_BIN)
	@$(NAMENODE_BIN)

run-datanode: build-datanode
	@chmod +x $(DATANODE_BIN)
	@$(DATANODE_BIN)

test:
	go test ./...

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/schema.proto

.PHONY: proto
