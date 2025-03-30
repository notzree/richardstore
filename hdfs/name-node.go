package hdfs

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
	"google.golang.org/protobuf/types/known/emptypb"
)

// Peer node view of NameNode
type PeerNameNode struct {
	Id      uint64
	Address string
	Client  *NameNodeClient
}

type NameNodeClient struct {
	conn   *grpc.ClientConn
	client proto.NameNodeClient
}

func NewNameNodeClient(address string) (*NameNodeClient, error) {
	log.Printf("Attempting to connect to namenode at %s", address)
	// Force the connection attempt to be blocking so we can see what's happening
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithTimeout(5*time.Second), // Add timeout so it doesn't hang forever
	)
	if err != nil {
		log.Printf("Failed to connect to %s: %v", address, err)
		return nil, err
	}

	log.Printf("Successfully connected to namenode at %s", address)

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
	server    *grpc.Server

	MaxSimCommand                  int
	HeartbeatInterval              time.Duration
	BlockReportInterval            time.Duration
	IncrementalBlockReportInterval time.Duration

	proto.UnimplementedNameNodeServer
}

func NewNameNode(Id uint64, address string, hbInterval time.Duration, blockReportInterval time.Duration, incrementalBlockReportInterval time.Duration, mxCmd int) *NameNode {
	initialCommands := make(map[uint64][]*proto.Command)
	dataNodeMap := make(map[uint64]*PeerDataNode)

	return &NameNode{
		Id:      Id,
		address: address,

		fmpMu: &sync.RWMutex{},
		Fmp:   *NewFileMap("./snapshot.bin", 1*time.Hour, 24*time.Hour),

		dnMu:      &sync.RWMutex{},
		DataNodes: dataNodeMap,

		cmdMu:    &sync.Mutex{},
		Commands: initialCommands,

		HeartbeatInterval:              hbInterval,
		BlockReportInterval:            blockReportInterval,
		IncrementalBlockReportInterval: incrementalBlockReportInterval,
		MaxSimCommand:                  mxCmd,
	}
}

func (node *NameNode) PeerRepresentation() *PeerNameNode {
	return &PeerNameNode{
		Id:      node.Id,
		Address: node.address,
		Client:  nil,
	}
}

func (node *NameNode) AddDataNodes(dataNodesSlice []PeerDataNode) {
	node.dnMu.Lock()
	defer node.dnMu.Unlock()

	for _, dn := range dataNodesSlice {
		dnCopy := dn
		node.DataNodes[dn.Id] = &dnCopy
		node.Commands[dn.Id] = make([]*proto.Command, 0)
	}
}

func (node *NameNode) Run() error {
	// Add more event loops as required
	return node.StartRPCServer()
}
func (node *NameNode) Stop() {
	// Create a context with timeout for graceful shutdown
	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Close any open connections
	if node.server != nil {
		node.server.GracefulStop()
	}

	// Save state if needed
	node.fmpMu.Lock()
	if err := node.Fmp.Snapshot(); err != nil {
		log.Printf("Error saving file map to disk: %v", err)
	}
	node.fmpMu.Unlock()

	log.Printf("NameNode %d stopped gracefully", node.Id)
}

func (node *NameNode) StartRPCServer() error {
	ln, err := net.Listen("tcp", node.address)
	if err != nil {
		return err
	}
	opts := []grpc.ServerOption{}
	node.server = grpc.NewServer(opts...)
	proto.RegisterNameNodeServer(node.server, node)
	log.Printf("starting name node server on port %s\n", node.address)
	return node.server.Serve(ln)
}

func (node *NameNode) WriteFile(ctx context.Context, req *proto.WriteFileRequest) (resp *proto.CreateFileResponse, err error) {
	node.dnMu.RLock()
	defer node.dnMu.RUnlock()

	incoming_file := req.FileInfo

	// record the initial file that the client sends to the name node.
	node.Fmp.Record(incoming_file)

	// calculate the number of required nodes to store this file
	numRequiredNodes := max(1, int(incoming_file.MinReplicationFactor*float32(len(node.DataNodes))))
	log.Printf("total number of nodes: %v", len(node.DataNodes))
	log.Printf("number of required nodes to store: %v", numRequiredNodes)
	// in the future I would make this a heap thing
	nodes := make([]*proto.DataNodeInfo, 0)
	for _, dataNode := range node.DataNodes {
		if len(nodes) >= numRequiredNodes {
			break
		}
		if !dataNode.Alive(node.HeartbeatInterval) || (dataNode.Capacity-dataNode.Used) < incoming_file.Size {
			continue
		}

		nodes = append(nodes, &proto.DataNodeInfo{Address: dataNode.Address})
	}
	log.Printf("telling client to write to %v nodes", len(nodes))
	if len(nodes) < numRequiredNodes {
		return nil, fmt.Errorf("insufficient available datanodes with capacity for replication factor %.0f: need %d, found %d",
			incoming_file.MinReplicationFactor, numRequiredNodes, len(nodes))
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
	fileEntry := node.Fmp.Has(req.Hash)
	if fileEntry == nil {
		log.Print("file not found\n")
		// do we return empty or throw error!?
		return &proto.ReadFileResponse{DataNodes: availableNodes, Size: 0}, nil
	}
	if len(fileEntry.Replicas) == 0 {
		// Case where the write is stored on the NameNode, but no DataNodes have sent a block report
		// or partial block report to indicate that they are storing the file
		log.Printf("file found but no data nodes have responded with write confirmation")
		return &proto.ReadFileResponse{DataNodes: availableNodes, Size: 0}, nil
	}
	for id := range fileEntry.Replicas {
		dn, exist := node.DataNodes[id]
		if !exist {
			return nil, fmt.Errorf("data node id not recognized: %d", id)
		}
		if !dn.Alive(node.HeartbeatInterval) {
			//todo: Should we remove this node from the replica?
			continue
		}
		availableNodes = append(availableNodes, &proto.DataNodeInfo{
			Address: dn.Address,
		})

	}
	if len(availableNodes) == 0 {
		return nil, fmt.Errorf("critical error, all replicas are down")
	}
	log.Printf("instructing client to read from: %v", availableNodes)
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
	file := node.Fmp.Delete(req.Hash)
	if file == nil {
		log.Println("tried to delete file that does not exist")
		//TODO: Return error here if tried to delete file that DNE?
		return &proto.DeleteFileResponse{Success: true}, nil
	}
	node.cmdMu.Lock()
	defer node.cmdMu.Unlock()
	for id := range file.Replicas {
		if _, exist := node.Commands[id]; !exist {
			return nil, fmt.Errorf("data node id not recognized: %d", id)
		}
		node.Commands[id] = append(node.Commands[id], &proto.Command{
			Command: &proto.Command_Delete{
				Delete: &proto.DeleteCommand{
					FileInfo: file.FileInfo,
				},
			},
		})
	}
	return &proto.DeleteFileResponse{Success: true}, nil
}

func (node *NameNode) BlockReport(ctx context.Context, req *proto.BlockReportRequest) (resp *proto.BlockReportResponse, err error) {
	node.dnMu.Lock()
	defer node.dnMu.Unlock()
	peerDataNode, exist := node.DataNodes[req.NodeId]
	if !exist {
		return nil, fmt.Errorf("data node id not recognized: %d", req.NodeId)
	}
	peerDataNode.LastSeen = time.Now()
	peerDataNode.Used = req.Used
	peerDataNode.Capacity = req.Capacity
	node.fmpMu.Lock()

	for _, file := range req.HeldFiles {
		node.Fmp.Record(file)
		node.Fmp.AddReplica(file, req.NodeId)
	}

	node.fmpMu.Unlock()
	if err != nil {
		return nil, err
		// try getting commands next iteration
		// TODO: what happens if this goes on forever???
		// return &proto.BlockReportResponse{
		// 	NodeId:   node.Id,
		// 	Commands: nil,
		// }, nil
	}
	fmt.Printf("succesfully received block report from %v", req.NodeId)
	return &proto.BlockReportResponse{
		NodeId:          node.Id,
		NextReportDelay: uint64(node.BlockReportInterval),
		ReportId:        req.LastReportId + 1,
	}, nil

}

func (node *NameNode) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (resp *proto.HeartbeatResponse, err error) {
	node.dnMu.Lock()
	defer node.dnMu.Unlock()
	if _, exist := node.DataNodes[req.NodeId]; !exist {
		node.DataNodes[req.NodeId] = &PeerDataNode{
			Id:       req.NodeId,
			Address:  req.Address, // actually port right now (feb 10th 2025)
			LastSeen: time.Now(),
			Capacity: req.Capacity,
			Used:     req.Used,
		}
		node.Commands[req.NodeId] = make([]*proto.Command, 0)
	}
	// log.Printf("received heartbeat from %v", req.NodeId)
	peerDataNode := node.DataNodes[req.NodeId]
	peerDataNode.Capacity = req.Capacity
	peerDataNode.Used = req.Used
	peerDataNode.LastSeen = time.Now()
	// get commands if any
	node.cmdMu.Lock()
	availableCommands, err := node.getCommands(req.NodeId)
	node.cmdMu.Unlock()
	if err != nil {
		// try getting commands next iteration
		// TODO: what happens if this goes on forever?
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
	// log.Printf("Handling incremental block report from %v", req.NodeId)
	node.dnMu.Lock()
	defer node.dnMu.Unlock()
	peerDataNode, exist := node.DataNodes[req.NodeId]
	if !exist {
		return nil, fmt.Errorf("data node id not recognized: %d", req.NodeId)
	}
	peerDataNode.LastSeen = time.Now()
	node.fmpMu.Lock()
	for _, update := range req.Updates {
		if update.Update == *proto.FileUpdate_UPDATE_ADD.Enum() {
			// adding a new file
			node.Fmp.Record(update.FileInfo)
			node.Fmp.AddReplica(update.FileInfo, req.NodeId)
		} else if update.Update == *proto.FileUpdate_UPDATE_DELETE.Enum() {
			// indicating that the data node has deleted a file that we have instructed it to delete
			if node.Fmp.Has(update.FileInfo.Hash) == nil {
				continue
			}
			// name node told data node to delete file (not really implemented for now)
			// but technically in hdfds this is used for rebalancing
			node.Fmp.RemoveReplica(update.FileInfo, req.NodeId)
		}
	}
	node.fmpMu.Unlock()

	if err != nil {
		// try getting commands next iteration
		// TODO: what happens if this goes on forever???
		return &proto.BlockReportResponse{
			NodeId: node.Id,
		}, nil
	}

	return &proto.BlockReportResponse{
		NodeId:          node.Id,
		NextReportDelay: uint64(node.IncrementalBlockReportInterval),
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

func (node *NameNode) Info(ctx context.Context, _ *emptypb.Empty) (*proto.NameNodeInfo, error) {
	node.dnMu.RLock()
	defer node.dnMu.RUnlock()

	dataNodes := make([]*proto.DataNodeInfo, 0, len(node.DataNodes))
	for _, dn := range node.DataNodes {
		if !dn.Alive(node.HeartbeatInterval) {
			continue
		}
		dataNodes = append(dataNodes, &proto.DataNodeInfo{
			Address: dn.Address,
		})
	}

	return &proto.NameNodeInfo{
		DataNodes: dataNodes,
	}, nil
}
