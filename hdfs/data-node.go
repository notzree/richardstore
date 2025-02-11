package hdfs

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/notzree/richardstore/proto"
	str "github.com/notzree/richardstore/store"
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
	Id   uint64
	port string

	statMu   *sync.Mutex
	Capacity uint64
	Used     uint64
	Storer   *str.Store

	dnMu      *sync.RWMutex
	DataNodes map[uint64]*PeerDataNode

	clientsMu *sync.RWMutex
	Clients   map[uint64]*DataNodeClient

	NameNode PeerNameNode
	cmdChan  chan *proto.Command // or whatever type you want to send

	errChan     chan error
	doneChan    chan interface{}
	MAX_RETRIES int

	// InitialHeartbeatInterval   time.Duration
	// InitialBlockReportInterval time.Duration
	proto.UnimplementedDataNodeServer
}

func NewDataNode(Id uint64, address string, storer *str.Store, maxSimCommands int, max_retires int, capacity ...uint64) *DataNode {
	var nodeCapacity uint64

	if len(capacity) > 0 {
		nodeCapacity = capacity[0]
	} else {
		nodeCapacity = storer.GetAvailableCapacity()
	}

	return &DataNode{
		Id:          Id,
		port:        address,
		Storer:      storer,
		statMu:      &sync.Mutex{},
		Capacity:    nodeCapacity,
		Used:        0,
		dnMu:        &sync.RWMutex{},
		DataNodes:   make(map[uint64]*PeerDataNode),
		clientsMu:   &sync.RWMutex{},
		Clients:     make(map[uint64]*DataNodeClient),
		cmdChan:     make(chan *proto.Command, maxSimCommands),
		errChan:     make(chan error, 1),
		MAX_RETRIES: max_retires,
		doneChan:    make(chan interface{}),
	}
}
func (node *DataNode) sendError(e error) {
	log.Printf("err %v", e)
	node.errChan <- e
	return
}
func (node *DataNode) Run() error {
	if err := node.InitializeClients(); err != nil {
		return err
	}

	go node.HandleCommands()
	go node.StartRPCServer()
	go node.HandleBlockReport()
	go node.HandleHeartbeat()
	select {
	case err := <-node.errChan:
		log.Printf("err detected!")
		// Initiate shutdown
		close(node.doneChan)
		return fmt.Errorf("server error: %w", err)
	}
}

func (node *DataNode) StartRPCServer() error {
	ln, err := net.Listen("tcp", node.port)
	if err != nil {
		return err
	}
	opts := []grpc.ServerOption{}
	server := grpc.NewServer(opts...)
	proto.RegisterDataNodeServer(server, node)
	log.Printf("starting data node server on port %s\n", node.port)
	go func() {
		server.Serve(ln)
	}()
	<-node.doneChan
	server.GracefulStop()
	return nil
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
	firstMsg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive initial command: %w", err)
	}
	resp, ok := firstMsg.Request.(*proto.WriteFileRequest_FileInfo)
	if !ok {
		return fmt.Errorf("first message must be a command")
	}

	expectedFileHash := resp.FileInfo.Hash

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

			chunk, ok := msg.Request.(*proto.WriteFileRequest_Chunk)
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

	log.Printf("wrote file %s to node %d", hash, node.Id)
	node.statMu.Lock()
	defer node.statMu.Unlock()
	node.Used += resp.FileInfo.Size
	stream.SendAndClose(&proto.WriteFileResponse{
		Success: true,
	})

	return nil
}

func (node *DataNode) ReadFile(req *proto.ReadFileRequest, stream grpc.ServerStreamingServer[proto.ReadFileStream]) error {
	file, err := node.Storer.Read(req.Hash)
	if err != nil {
		return err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	// Send Read File Info as first message
	err = stream.Send(&proto.ReadFileStream{
		Request: &proto.ReadFileStream_FileInfo{
			FileInfo: &proto.DataNodeFile{
				Hash:              req.Hash,
				Size:              uint64(fileInfo.Size()),
				ModificationStamp: uint64(fileInfo.ModTime().Unix()),
			},
		},
	})
	if err != nil {
		return err
	}

	// Stream the rest of the data over in a buffer
	buf := make([]byte, 1024)
	for {
		_, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		err = stream.Send(&proto.ReadFileStream{
			Request: &proto.ReadFileStream_Chunk{
				Chunk: buf,
			},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (node *DataNode) SendHeartbeat() (time.Duration, error) {
	c := node.NameNode.Client
	ctx := context.Background()
	response, err := c.client.Heartbeat(ctx, &proto.HeartbeatRequest{
		NodeId:   node.Id,
		Capacity: node.Capacity,
		Used:     node.Used,
		Address:  node.port, //TODO: Might need to change this, should be aight thru docker-compose tho
	})
	if err != nil {
		return 0, err
	}
	for _, cmd := range response.Commands {
		node.cmdChan <- cmd
	}

	return time.Duration(response.NextHeartbeatDelay), nil
}
func (node *DataNode) HandleHeartbeat() {
	// max consecutive retries
	MAX_RETRIES := node.MAX_RETRIES
	interval, err := node.SendHeartbeat()
	if err != nil {
		log.Printf("err sending heartbeat %s", err)
		node.sendError(err)
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-node.doneChan:
			return
		case <-ticker.C:
			newInterval, err := node.SendHeartbeat()
			if err != nil {
				log.Println("block report error", err)
				MAX_RETRIES -= 1
				if MAX_RETRIES <= 0 {
					node.sendError(err)
					return
				}
			}
			MAX_RETRIES += 1 // Reset retry count on successful heartbeat
			if newInterval != interval {
				ticker.Reset(newInterval)
				interval = newInterval
			}
		}
	}
}

func (node *DataNode) SendBlockreport() (time.Duration, error) {
	c := node.NameNode.Client
	ctx := context.Background()
	rawFiles, err := node.Storer.Stat()
	if err != nil {
		return 0, err
	}
	heldFiles := make([]*proto.DataNodeFile, len(rawFiles))
	for i, file := range rawFiles {
		fileInfo, err := file.Stat()
		if err != nil {
			return 0, err
		}
		heldFiles[i] = &proto.DataNodeFile{
			Hash:              fileInfo.Name(),
			Size:              uint64(fileInfo.Size()),
			ModificationStamp: uint64(fileInfo.ModTime().Unix()),
		}
		file.Close()
	}

	response, err := c.client.BlockReport(ctx, &proto.BlockReportRequest{
		NodeId:       node.Id,
		Capacity:     node.Capacity,
		Used:         node.Used,
		Timestamp:    uint64(time.Now().Unix()),
		HeldFiles:    heldFiles,
		LastReportId: 0, // not implemented or used currently
	})
	if err != nil {
		return 0, err
	}
	for _, cmd := range response.Commands {
		node.cmdChan <- cmd
	}
	return time.Duration(response.NextReportDelay), nil

}

func (node *DataNode) HandleBlockReport() {
	// max consecutive retries
	MAX_RETRIES := node.MAX_RETRIES
	interval := 30 * time.Second // Default interval for initial failure case

	// Try first block report
	newInterval, err := node.SendBlockreport()
	if err != nil {
		log.Printf("err sending initial block report %s", err)
		MAX_RETRIES -= 1
	} else {
		interval = newInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-node.doneChan:
			return
		case <-ticker.C:
			newInterval, err := node.SendBlockreport()
			if err != nil {
				log.Println(err)
				MAX_RETRIES -= 1
				if MAX_RETRIES <= 0 {
					node.sendError(err)
					return
				}
			} else {
				MAX_RETRIES = node.MAX_RETRIES // Reset retry count on success
				if newInterval != interval {
					ticker.Reset(newInterval)
					interval = newInterval
				}
			}
		}
	}
}

// Handle + process any commands
func (node *DataNode) HandleCommands() {
	for {
		select {
		case <-node.doneChan:
			return
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
	node.statMu.Lock()
	defer node.statMu.Unlock()
	node.Used -= cmd.FileInfo.Size

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
		Address:  node.port,
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
	if node.NameNode.Client == nil {
		c, err := NewNameNodeClient(node.NameNode.Address)
		if err != nil {
			return err
		}
		node.NameNode.Client = c
	}

	// Connect w/ peers
	for id, peer := range node.DataNodes {
		if peer == nil {
			continue
		}
		c, err := NewDataNodeClient(peer.Address)
		if err != nil {
			return err
		}
		// log.Printf("%d connected to node %d\n", node.Id, peer.Id)
		node.Clients[id] = c
	}
	log.Printf("node id: %d connected to %d other peer nodes", node.Id, len(node.DataNodes))
	return nil

}
