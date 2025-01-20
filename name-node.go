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
	"google.golang.org/grpc/credentials/insecure"
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

// Peer node view of NameNode
type PeerNameNode struct {
	Id      uint64
	address string
	client  NameNodeClient
}

type NameNodeClient struct {
	conn   *grpc.ClientConn
	client proto.NameNodeClient
}

func NewNameNodeClient(address string) (*NameNodeClient, error) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &NameNodeClient{
		conn:   conn,
		client: proto.NewNameNodeClient(conn),
	}, nil
}

func (nc *NameNodeClient) Close() error {
	if nc.conn != nil {
		return nc.conn.Close()
	}
	return nil
}

type NameNode struct {
	Id        uint64
	address   string
	fmpMu     *sync.RWMutex
	Fmp       FileMap
	dnMu      *sync.RWMutex
	DataNodes map[uint64]*PeerDataNode
	cmdMu     *sync.Mutex
	Commands  map[uint64][]*proto.Command

	MaxSimCommand       int
	HeartbeatInterval   time.Duration
	BlockReportInterval time.Duration

	proto.UnimplementedNameNodeServer
}

func NewNameNode(Id uint64, address string, dataNodesSlice []PeerDataNode, hbInterval time.Duration, mxCmd int) *NameNode {

	dataNodeMap := make(map[uint64]*PeerDataNode)
	for _, dn := range dataNodesSlice {
		dnCopy := dn
		dataNodeMap[dn.Id] = &dnCopy
	}

	return &NameNode{
		Id:      Id,
		address: address,

		fmpMu: &sync.RWMutex{},
		Fmp:   make(FileMap),

		dnMu:      &sync.RWMutex{},
		DataNodes: dataNodeMap,

		cmdMu:    &sync.Mutex{},
		Commands: make(map[uint64][]*proto.Command),

		HeartbeatInterval: hbInterval,
		MaxSimCommand:     mxCmd,
	}
}

func (node *NameNode) PeerRepresentation() *PeerNameNode {
	c, err := NewNameNodeClient(node.address)
	if err != nil {
		panic("failed to initiate name node connection")
	}
	return &PeerNameNode{
		Id:      node.Id,
		address: node.address,
		client:  *c,
	}
}

func (node *NameNode) AddDataNodes(dataNodesSlice []PeerDataNode) {
	node.dnMu.Lock()
	defer node.dnMu.Unlock()

	for _, dn := range dataNodesSlice {
		dnCopy := dn
		node.DataNodes[dn.Id] = &dnCopy
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
	node.dnMu.RLock()
	defer node.dnMu.RUnlock()
	nodes := make([]*proto.DataNodeInfo, 0)

	numRequiredNodes := int(int(req.MinReplicationFactor) * len(node.DataNodes))
	// in the future I would make this a heap thing
	for _, dataNode := range node.DataNodes {
		if !dataNode.Alive || (dataNode.Capacity-dataNode.Used) < req.Size {
			continue
		}
		nodes = append(nodes, &proto.DataNodeInfo{Address: dataNode.Address})
		if len(nodes) >= numRequiredNodes {
			break
		}
	}
	if len(nodes) < numRequiredNodes {
		return nil, fmt.Errorf("insufficient available datanodes for replication factor %d: need %d, found %d",
			req.MinReplicationFactor, numRequiredNodes, len(nodes))
	}
	return &proto.CreateFileResponse{
		DataNodes: nodes,
	}, nil
}

func (node *NameNode) ReadFile(ctx context.Context, req *proto.ReadFileRequest) (resp *proto.ReadFileResponse, err error) {
	node.fmpMu.RLock()
	defer node.fmpMu.RUnlock()
	node.dnMu.RLock()
	defer node.dnMu.RUnlock()
	availableNodes := make([]*proto.DataNodeInfo, 0)
	fileEntry, exist := node.Fmp[req.Hash]
	if !exist {
		// do we return empty or throw error!?
		return &proto.ReadFileResponse{DataNodes: availableNodes, Size: 0}, nil
	}
	for id, _ := range fileEntry.Replicas {
		dn, exist := node.DataNodes[id]
		if !exist {
			return nil, fmt.Errorf("data node id not recognized: %d", id)
		}
		availableNodes = append(availableNodes, &proto.DataNodeInfo{
			Address: dn.Address,
		})

	}
	return &proto.ReadFileResponse{
		Size:      fileEntry.Size,
		DataNodes: availableNodes,
	}, nil

}

func (node *NameNode) DeleteFile(ctx context.Context, req *proto.DeleteFileRequest) (resp *proto.DeleteFileResponse, err error) {
	node.fmpMu.RLock()
	defer node.fmpMu.RUnlock()
	node.dnMu.Lock()
	defer node.dnMu.Unlock()
	fileEntry, exist := node.Fmp[req.Hash]
	if !exist {
		return &proto.DeleteFileResponse{Success: false}, nil
	}
	node.cmdMu.Lock()
	defer node.cmdMu.Unlock()
	for id, _ := range fileEntry.Replicas {
		if _, exist := node.Commands[id]; !exist {
			return nil, fmt.Errorf("data node id not recognized: %d", id)
		}
		node.Commands[id] = append(node.Commands[id], &proto.Command{
			Command: &proto.Command_Delete{
				Delete: &proto.DeleteCommand{
					FileInfo: fileEntry.FileInfo,
				},
			},
		})
	}
	return &proto.DeleteFileResponse{Success: true}, nil
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
