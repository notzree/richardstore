package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/notzree/richardstore/proto"
	"google.golang.org/grpc"
)

type PeerDataNode struct {
	Id         uint64
	RPCAddress string
	Alive      bool
	LastSeen   time.Time
	// in bytes
	Capacity uint64
	Used     uint64
}

type DataNode struct {
	Id      uint64
	address string

	Capacity uint64
	Used     uint64
	Storer   *Store

	HeartbeatInterval   time.Duration
	BlockReportInterval time.Duration
	proto.UnimplementedDataNodeServer
}

func NewDataNode(Id uint64, address string, storer *Store, capacity ...uint64) *DataNode {
	var nodeCapacity uint64

	if len(capacity) > 0 {
		nodeCapacity = capacity[0]
	} else {
		nodeCapacity = storer.GetAvailableCapacity()
	}

	return &DataNode{
		Id:       Id,
		address:  address,
		Storer:   storer,
		Capacity: nodeCapacity,
		Used:     0,
	}
}

func (node *DataNode) StartRPCServer() error {
	ln, err := net.Listen("tcp", node.address)
	if err != nil {
		return err
	}
	opts := []grpc.ServerOption{}
	server := grpc.NewServer(opts...)
	proto.RegisterDataNodeServer(server, node)
	log.Printf("starting data node server on port %s\n", node.address)
	return server.Serve(ln)
}

func (node *DataNode) HandleDeleteCommand(ctx context.Context, cmd *proto.DeleteCommand) (*proto.CommandResponse, error) {
	// TODO: Delete file from Storer

}

func (node *DataNode) ReplicateFile(stream proto.DataNode_ReplicateFileServer) (*proto.CommandResponse, error) {
	// TODO: Handle replication stream similar to WriteFile
	// But likely also need to handle forwarding to next DataNode in pipeline
}

func (node *DataNode) WriteFile(stream proto.DataNode_WriteFileServer) (*proto.WriteFileResponse, error) {
	// TODO: Receive file info and chunkss
	// Write to Storer
}

func (node *DataNode)ReadFile(*proto.ReadFileRequest, grpc.ServerStreamingServer[*proto.ReadFileResponse]) error{}
	// TODO: Read file from Storer
	// Stream chunks back to client
}

// Optional helper function for handling streaming writes
func (dn *DataNode) handleFileStream(stream proto.DataNode_WriteFileServer) (*proto.FileInfo, error) {
	// Common streaming logic that could be used by both WriteFile and ReplicateFile
	return nil, fmt.Errorf("not implemented")
}
