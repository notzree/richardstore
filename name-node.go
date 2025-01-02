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

type FileEntry struct {
	*proto.FileInfo                        // fields of FileInfo
	Replicas        map[uint64]interface{} // set containing list of data node ids that have the file
}

func NewFileEntry(fi *proto.FileInfo) *FileEntry {
	return &FileEntry{
		FileInfo: fi,
		Replicas: make(map[uint64]interface{}),
	}
}

// FileMap maps file hash -> FileEntry
type FileMap map[string]*FileEntry
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
	// in bytes
	Capacity uint64
	Used     uint64
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

	MaxSimCommand       int
	HeartbeatInterval   time.Duration
	BlockReportInterval time.Duration

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
	// block report
	node.dnMu.Lock()
	defer node.dnMu.Unlock()
	peerDataNode, exist := node.DataNodes[req.NodeId]
	if !exist {
		return nil, fmt.Errorf("data node id not recognized: %d", req.NodeId)
	}
	peerDataNode.Alive = true
	peerDataNode.LastSeen = time.Now()
	peerDataNode.Used = req.Used
	peerDataNode.Capacity = req.Capacity
	node.fmpMu.Lock()
	for _, file := range req.HeldFiles {
		// for each file the data node tells us about,
		// add it to the map
		// create if not exist or assign newer file info
		if _, exist := node.Fmp[file.Hash]; !exist {
			node.Fmp[file.Hash] = NewFileEntry(file)
		}
		// add the node id into the list of replicas for the file
		node.Fmp[file.Hash].Replicas[req.NodeId] = struct{}{}
	}
	node.fmpMu.Unlock()

	node.cmdMu.Lock()
	availableCommands, err := node.getCommands(req.NodeId)
	node.cmdMu.Unlock()
	if err != nil {
		// try getting commands next iteration
		// TODO: what happens if this goes on forever???
		return &proto.BlockReportResponse{
			NodeId:   node.Id,
			Commands: nil,
		}, nil
	}

	return &proto.BlockReportResponse{
		NodeId:          node.Id,
		Commands:        availableCommands,
		NextReportDelay: uint64(node.BlockReportInterval),
		ReportId:        req.LastReportId + 1,
	}, nil

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
	node.cmdMu.Unlock()
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
	// block report
	node.dnMu.Lock()
	defer node.dnMu.Unlock()
	peerDataNode, exist := node.DataNodes[req.NodeId]
	if !exist {
		return nil, fmt.Errorf("data node id not recognized: %d", req.NodeId)
	}
	peerDataNode.Alive = true
	peerDataNode.LastSeen = time.Now()
	node.fmpMu.Lock()
	for _, update := range req.Updates {
		fileHash := update.FileInfo.Hash
		if _, exist := node.Fmp[fileHash]; !exist {
			node.Fmp[fileHash] = NewFileEntry(update.FileInfo)
		}
		if update.Update == *proto.FileUpdate_UPDATE_ADD.Enum() {
			node.Fmp[fileHash].Replicas[req.NodeId] = struct{}{}
		} else if update.Update == *proto.FileUpdate_UPDATE_DELETE.Enum() {
			if _, exist := node.Fmp[fileHash].Replicas[node.Id]; exist {
				delete(node.Fmp[fileHash].Replicas, node.Id)
			}
		}
	}
	node.fmpMu.Unlock()

	node.cmdMu.Lock()
	availableCommands, err := node.getCommands(req.NodeId)
	node.cmdMu.Unlock()
	if err != nil {
		// try getting commands next iteration
		// TODO: what happens if this goes on forever???
		return &proto.BlockReportResponse{
			NodeId:   node.Id,
			Commands: nil,
		}, nil
	}

	return &proto.BlockReportResponse{
		NodeId:          node.Id,
		Commands:        availableCommands,
		NextReportDelay: uint64(node.BlockReportInterval),
	}, nil

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
