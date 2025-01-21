package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/notzree/richardstore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// peer nodes view of the data node
type PeerDataNode struct {
	Id       uint64
	Address  string
	Alive    bool
	LastSeen time.Time
	// in bytes
	Capacity uint64
	Used     uint64
}
type DataNodeClient struct {
	conn   *grpc.ClientConn
	client proto.DataNodeClient
}

func NewDataNodeClient(address string) (*DataNodeClient, error) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &DataNodeClient{
		conn:   conn,
		client: proto.NewDataNodeClient(conn),
	}, nil
}

func (dc *DataNodeClient) Close() error {
	if dc.conn != nil {
		return dc.conn.Close()
	}
	return nil
}

type DataNode struct {
	Id      uint64
	address string

	Capacity uint64
	Used     uint64
	Storer   *Store

	dnMu      *sync.RWMutex
	DataNodes map[uint64]*PeerDataNode

	clientsMu *sync.RWMutex
	Clients   map[uint64]*DataNodeClient

	NameNode PeerNameNode
	cmdChan  chan *proto.Command // or whatever type you want to send

	HeartbeatInterval   time.Duration
	BlockReportInterval time.Duration
	proto.UnimplementedDataNodeServer
}

func NewDataNode(Id uint64, address string, storer *Store, maxSimCommands int, capacity ...uint64) *DataNode {
	var nodeCapacity uint64

	if len(capacity) > 0 {
		nodeCapacity = capacity[0]
	} else {
		nodeCapacity = storer.GetAvailableCapacity()
	}

	return &DataNode{
		Id:        Id,
		address:   address,
		Storer:    storer,
		Capacity:  nodeCapacity,
		Used:      0,
		dnMu:      &sync.RWMutex{},
		DataNodes: make(map[uint64]*PeerDataNode),
		clientsMu: &sync.RWMutex{},
		Clients:   make(map[uint64]*DataNodeClient),
		cmdChan:   make(chan *proto.Command, maxSimCommands),
	}
}
func (node *DataNode) Run() error {
	node.InitializeClients()
	go node.HandleCommands()
	return node.StartRPCServer()
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

// handle replication stream (datanode -> peer datanode)
func (node *DataNode) ReplicateFile(stream grpc.ClientStreamingServer[proto.ReplicateFileRequest, proto.CommandResponse]) error {
	// Get the first message containing the command
	firstMsg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive initial command: %w", err)
	}

	repCmd, ok := firstMsg.Request.(*proto.ReplicateFileRequest_Command)
	if !ok {
		return fmt.Errorf("first message must be a command")
	}

	cmd := repCmd.Command
	expectedFileHash := cmd.FileInfo.Hash

	// Create a pipe to connect the stream to an io.Reader
	pr, pw := io.Pipe()

	// Start a goroutine to read from the stream and write to the pipe
	go func() {
		defer pw.Close() // Make sure we close the writer when done

		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				pw.CloseWithError(fmt.Errorf("error receiving chunk: %w", err))
				return
			}

			chunk, ok := msg.Request.(*proto.ReplicateFileRequest_Chunk)
			if !ok {
				pw.CloseWithError(fmt.Errorf("expected chunk message"))
				return
			}

			_, err = pw.Write(chunk.Chunk)
			if err != nil {
				pw.CloseWithError(fmt.Errorf("error writing to pipe: %w", err))
				return
			}
		}
	}()

	// Pass the reader end of the pipe to Write()
	hash, err := node.Storer.Write(pr)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	if hash != expectedFileHash {
		return fmt.Errorf("file hash mismatch expecte %s got %s", expectedFileHash, hash)
	}
	if len(cmd.TargetNodes) > 0 {
		node.cmdChan <- &proto.Command{
			Command: &proto.Command_Replicate{
				Replicate: cmd,
			},
		}
	}
	log.Printf("File replication completed for node %d", node.Id)
	return stream.SendAndClose(&proto.CommandResponse{
		Success: true,
	})
}

func (node *DataNode) WriteFile(stream grpc.ClientStreamingServer[proto.WriteFileRequest, proto.WriteFileResponse]) error {
	return nil
}

func (node *DataNode) ReadFile(req *proto.ReadFileRequest, stream grpc.ServerStreamingServer[proto.ReadFileResponse]) error {
	return nil
}

// TODO: Read file from Storer
// Stream chunks back to client

// Optional helper function for handling streaming writes
// func (node *DataNode) handleFileStream(stream proto.DataNode_WriteFileServer) (*proto.FileInfo, error) {
// 	// Common streaming logic that could be used by both WriteFile and ReplicateFile
// 	return nil, fmt.Errorf("not implemented")
// }

// SendHeartbeat will send a heartbeat to the server,
// and enque any commands to the data node once it gets a response.

func (node *DataNode) SendHeartbeat() error {
	c := node.NameNode.client
	ctx := context.Background()
	response, err := c.client.Heartbeat(ctx, &proto.HeartbeatRequest{
		NodeId:   node.Id,
		Capacity: node.Capacity,
		Used:     node.Used,
	})
	if err != nil {
		return err
	}
	for _, cmd := range response.Commands {
		node.cmdChan <- cmd
	}

	node.HeartbeatInterval = time.Duration(response.NextHeartbeatDelay)
	return nil
}

// Handle + process any commands
func (node *DataNode) HandleCommands() error {
	for {
		select {
		case cmd := <-node.cmdChan:
			log.Printf("received cmd %v", cmd)
			switch c := cmd.Command.(type) {
			case *proto.Command_Replicate:
				err := node.handleReplication(c.Replicate)
				if err != nil {
					log.Printf("Error handling replication: %v", err)
				}

			case *proto.Command_Delete:
				err := node.handleDelete(c.Delete)
				if err != nil {
					log.Printf("Error handling delete: %v", err)
				}
			default:
				log.Printf("Unknown command type: %T", c)
			}
		}
	}
}

func (node *DataNode) handleDelete(cmd *proto.DeleteCommand) error {
	err := node.Storer.Delete(cmd.FileInfo.Hash)
	if err != nil {
		return err
	}
	// might want to grab the file size again from the file system rather than trusting cmd
	node.Used += cmd.FileInfo.Size
	return nil
}

func (node *DataNode) handleReplication(cmd *proto.ReplicateCommand) error {
	if !node.Storer.Has(cmd.FileInfo.Hash) {
		return fmt.Errorf("node %d missing file hash %s", node.Id, cmd.FileInfo.Hash)
	}

	// Get the file reader
	fr, err := node.Storer.Read(cmd.FileInfo.Hash)
	if err != nil {
		return err
	}
	defer fr.Close()
	log.Printf("remaining targets %d", len(cmd.TargetNodes))
	// Might not need this here tbh
	if len(cmd.TargetNodes) == 0 {
		log.Printf("end of replication chain reached with node %d", node.Id)
		return nil
	}
	target := cmd.TargetNodes[0]
	remaining := cmd.TargetNodes[1:]
	node.clientsMu.Lock()
	defer node.clientsMu.Unlock()

	ctx := context.Background()
	stream, err := node.Clients[target].client.ReplicateFile(ctx)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}
	log.Printf("sending first message to node: %d\n", target)
	// First send the command
	err = stream.Send(&proto.ReplicateFileRequest{
		Request: &proto.ReplicateFileRequest_Command{
			Command: &proto.ReplicateCommand{
				FileInfo:                 cmd.FileInfo,
				TargetNodes:              remaining, // Pass remaining nodes
				CurrentReplicationFactor: cmd.CurrentReplicationFactor,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to send command: %w", err)
	}

	// Then stream the file chunks
	buf := make([]byte, 32*1024) // 32KB chunks
	for {
		n, err := fr.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading file: %w", err)
		}

		err = stream.Send(&proto.ReplicateFileRequest{
			Request: &proto.ReplicateFileRequest_Chunk{
				Chunk: buf[:n],
			},
		})
		if err != nil {
			return fmt.Errorf("failed to send chunk: %w", err)
		}
	}

	// Close the stream and get response
	response, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("error closing stream: %w", err)
	}
	if !response.Success {
		return fmt.Errorf("failed to replicate file: %s to node: %d", cmd.FileInfo.Hash, target)
	}

	return nil
}

func (node *DataNode) CloseAllClients() {
	node.clientsMu.Lock()
	defer node.clientsMu.Unlock()

	for _, client := range node.Clients {
		client.Close()
	}
	node.Clients = make(map[uint64]*DataNodeClient)
}

func (node *DataNode) PeerRepresentation() *PeerDataNode {
	return &PeerDataNode{
		Id:       node.Id,
		Address:  node.address,
		Alive:    true,
		LastSeen: time.Now(),
		// in bytes
		Capacity: node.Capacity,
		Used:     node.Used,
	}
}

func (node *DataNode) AddDataNodes(dataNodesSlice []PeerDataNode) {
	node.dnMu.Lock()
	defer node.dnMu.Unlock()

	for _, dn := range dataNodesSlice {
		dnCopy := dn
		node.DataNodes[dn.Id] = &dnCopy
	}
}

func (node *DataNode) InitializeClients() error {
	node.clientsMu.Lock()
	defer node.clientsMu.Unlock()
	// Create name node connection
	if node.NameNode.client == nil {
		c, err := NewNameNodeClient(node.NameNode.address)
		if err != nil {
			return err
		}
		node.NameNode.client = c
	}

	// Connect w/ peers
	for id, peer := range node.DataNodes {
		c, err := NewDataNodeClient(peer.Address)
		if err != nil {
			return err
		}
		// log.Printf("%d connected to node %d\n", node.Id, peer.Id)
		node.Clients[id] = c
	}
	log.Printf("%d connected to %d other peer nodes", node.Id, len(node.DataNodes))
	return nil

}

//NEXT STEPS:
// Finish up 2nd half of replication process
// Work on writing files
// write a wrapper to start rpc server, initialize clients, poll commands
