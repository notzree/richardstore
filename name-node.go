package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/notzree/richardstore/proto"
	"google.golang.org/grpc"
)

type FileMap map[string][]uint64
type NodeType int

const (
	Name NodeType = iota
	Data
)

// Name-nodes view of the data node
type PeerDataNode struct {
	Id         uint64
	RPCAddress string
	Alive      bool
	LastSeen   time.Time
	Capacity   float32
	Used       float32
}

type NameNode struct {
	Id        uint64
	address   string
	fmpMu     *sync.Mutex
	Fmp       FileMap
	dnMu      *sync.Mutex
	DataNodes map[uint64]*PeerDataNode
	cmdMu     *sync.Mutex
	Commands  map[uint64][]*proto.Command

	MaxSimCommand     int
	HeartbeatInterval time.Duration

	Storer *Store

	proto.UnimplementedNameNodeServer
}

func NewNameNode(Id uint64, address string, storer *Store, dataNodesSlice []PeerDataNode, hbInterval time.Duration, mxCmd int) *NameNode {

	dataNodeMap := make(map[uint64]*PeerDataNode)
	for _, dn := range dataNodesSlice {
		dnCopy := dn
		dataNodeMap[dn.Id] = &dnCopy
	}

	return &NameNode{
		Id:        Id,
		address:   address,
		Storer:    storer,
		fmpMu:     &sync.Mutex{},
		Fmp:       make(FileMap),
		dnMu:      &sync.Mutex{},
		DataNodes: dataNodeMap,
		cmdMu:     &sync.Mutex{},
		Commands:  make(map[uint64][]*proto.Command),

		HeartbeatInterval: hbInterval,
		MaxSimCommand:     mxCmd,
	}
}

func (node *NameNode) StartRPCServer() error {
	ln, err := net.Listen("tcp", node.address)
	if err != nil {
		return err
	}
	opts := []grpc.ServerOption{}
	server := grpc.NewServer(opts...)
	proto.RegisterNameNodeServer(server, node)
	log.Printf("starting name node server on port %s\n", node.address)
	return server.Serve(ln)
}

func (node *NameNode) CreateFile(ctx context.Context, req *proto.CreateFileRequest) (resp *proto.CreateFileResponse, err error) {

}

func (node *NameNode) ReadFile(ctx context.Context, req *proto.ReadFileRequest) (resp *proto.ReadFileResponse, err error) {

}

func (node *NameNode) DeleteFile(ctx context.Context, req *proto.DeleteFileRequest) (resp *proto.DeleteFileResponse, err error) {

}

// Consensus things
func (node *NameNode) BlockReport(ctx context.Context, req *proto.BlockReportRequest) (resp *proto.BlockReportResponse, err error) {

}

func (node *NameNode) HeartBeat(ctx context.Context, req *proto.HeartbeatRequest) (resp *proto.HeartbeatResponse, err error) {
	node.dnMu.Lock()
	defer node.dnMu.Unlock()
	peerDataNode, exist := node.DataNodes[req.NodeId]
	if !exist {
		return nil, fmt.Errorf("data node id not recognized: %d", req.NodeId)
	}
	peerDataNode.Alive = true
	peerDataNode.Capacity = req.Capacity
	peerDataNode.Used = req.Used
	peerDataNode.LastSeen = time.Now()
	// get commands if any
	node.cmdMu.Lock()
	availableCommands, err := node.getCommands(req.NodeId)
	if err != nil {
		// try getting commands next iteration
		// TODO: what happens if this goes on forever???
		return &proto.HeartbeatResponse{
			NodeId:             node.Id,
			NextHeartbeatDelay: uint64(node.HeartbeatInterval),
			Commands:           nil,
		}, nil
	}

	return &proto.HeartbeatResponse{
		NodeId:             node.Id,
		NextHeartbeatDelay: uint64(node.HeartbeatInterval), // maybe add more complex logic later
		Commands:           availableCommands,
	}, nil

}

func (node *NameNode) IncrementalBlockReport(ctx context.Context, req *proto.IncrementalBlockReportRequest) (reqp *proto.BlockReportResponse, err error) {

}

// getCommands should get all available commands
// Must be called within a cmdMu.Lock()
func (node *NameNode) getCommands(id uint64) (cmds []*proto.Command, err error) {
	commands, exist := node.Commands[id]
	if !exist {
		return nil, fmt.Errorf("data node id not recognized: %d", id)
	}
	maxCommandLength := min(node.MaxSimCommand, len(commands))
	commands = commands[:maxCommandLength] //shrink to maximum simuealtaeous command limit
	node.Commands[id] = node.Commands[id][maxCommandLength:]

	return commands, nil

}
