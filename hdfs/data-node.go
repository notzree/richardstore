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

type DataNodeState int

const (
	SafeMode DataNodeState = iota
	Active
)

type DataNode struct {
	Id          uint64
	address     string
	MAX_RETRIES int

	state   DataNodeState
	stateMu *sync.RWMutex

	statMu   *sync.Mutex
	Capacity uint64
	Used     uint64
	Storer   *str.Store

	NameNode PeerNameNode

	cmdChan  chan *proto.Command // or whatever type you want to send
	errChan  chan error
	doneChan chan interface{}

	cLogMu    *sync.Mutex
	ChangeLog []*proto.FileUpdate

	// InitialHeartbeatInterval   time.Duration
	// InitialBlockReportInterval time.Duration
	proto.UnimplementedDataNodeServer
}

func NewDataNode(Id uint64, address string, storer *str.Store, maxSimCommands int, max_retires int, capacity *uint64) *DataNode {
	var nodeCapacity uint64

	if capacity != nil {
		nodeCapacity = *capacity
	} else {
		nodeCapacity = storer.GetAvailableCapacity()
	}
	return &DataNode{
		Id:          Id,
		address:     address,
		Storer:      storer,
		statMu:      &sync.Mutex{},
		Capacity:    nodeCapacity,
		Used:        0,
		cmdChan:     make(chan *proto.Command, maxSimCommands),
		errChan:     make(chan error, 1),
		MAX_RETRIES: max_retires,
		doneChan:    make(chan interface{}),
		state:       SafeMode,
		stateMu:     &sync.RWMutex{},
	}
}
func (node *DataNode) sendError(e error) {
	log.Printf("err %v", e)
	node.errChan <- e
}
func (node *DataNode) Run() error {
	if err := node.InitializeClients(); err != nil {
		return err
	}
	go node.HandleHeartbeat()
	for {
		// stay in Safe Mode until node.state is active
		node.stateMu.RLock()
		if node.state == Active {
			break
		}
		node.stateMu.RUnlock()
		time.Sleep(1 * time.Second)
	}
	log.Printf("NODE ACTIVE!")
	go node.StartRPCServer()
	go node.HandleCommands()
	go node.HandleBlockReport()
	select {
	case err := <-node.errChan:
		log.Printf("err detected!")
		// Initiate shutdown
		close(node.doneChan)
		return fmt.Errorf("server error: %w", err)
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
	go func() {
		server.Serve(ln)
	}()
	<-node.doneChan
	server.GracefulStop()
	return nil
}

// write file and handle replication stream (datanode -> peer datanode)
func (node *DataNode) WriteFile(stream grpc.ClientStreamingServer[proto.FileStream, proto.WriteFileResponse]) error {
	firstMsg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive initial command: %w", err)
	}

	streamType, ok := firstMsg.Type.(*proto.FileStream_StreamInfo)
	if !ok {
		return fmt.Errorf("first message must contain stream info!")
	}

	info := streamType.StreamInfo
	expectedFileHash := info.FileInfo.Hash

	var replicator *FilestreamReplicator

	if len(info.DataNodes) >= 0 {
		downstreamNodeInfo := info.DataNodes[0]
		downstreamInfo := &proto.StreamInfo{
			FileInfo:  info.FileInfo,
			DataNodes: info.DataNodes[1:],
		}
		ctx := context.Background()
		replicator, err = NewReplicator(ctx, downstreamNodeInfo.Address, downstreamInfo)
		if err != nil {
			return fmt.Errorf("failed to initiate replication chain %v", err)
		}
	}

	// Create a pipe to connect the stream to an io.Reader
	pr, pw := io.Pipe()

	// Start a goroutine to read from the stream and write to the pipe
	go func() {
		defer pw.Close()
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				pw.CloseWithError(fmt.Errorf("error receiving chunk: %w", err))
				return
			}

			chunk, ok := msg.Type.(*proto.FileStream_Chunk)
			if !ok {
				pw.CloseWithError(fmt.Errorf("expected chunk message"))
				return
			}

			_, err = pw.Write(chunk.Chunk)
			if err != nil {
				pw.CloseWithError(fmt.Errorf("error writing to pipe: %w", err))
				return
			}
			// write chunk to downstream node if exist
			if replicator != nil {
				replicator.SendChunk(&proto.FileStream{
					Type: chunk,
				})
			}
		}
	}()
	hash, err := node.Storer.Write(pr)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	response, err := replicator.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("err closing replication stream: %v", err)
	}
	if !response.Success {
		// TODO: Should we delete this to avoid partial failures?
		return fmt.Errorf("failed to replicate to downstream nodes %v", err)
	}
	if hash != expectedFileHash {
		err := node.Storer.Delete(hash) // don't save corrupt data
		didDelete := true
		if err != nil {
			didDelete = false
			fmt.Errorf("failed to orphaned file with hash %v", hash)
		}
		return fmt.Errorf("file hash mismatch expecte %s got %s | did delete?:", expectedFileHash, hash, didDelete)
	}

	node.statMu.Lock()
	defer node.statMu.Unlock()
	node.Used += info.FileInfo.Size

	node.cLogMu.Lock()
	defer node.cLogMu.Unlock()
	node.ChangeLog = append(node.ChangeLog, &proto.FileUpdate{
		FileInfo: info.FileInfo,
		Update:   *proto.FileUpdate_UPDATE_ADD.Enum(),
	})

	log.Printf("File replication completed for node %d", node.Id)
	return stream.SendAndClose(&proto.WriteFileResponse{
		Success: true,
	})
}

func (node *DataNode) ReadFile(req *proto.ReadFileRequest, stream grpc.ServerStreamingServer[proto.FileStream]) error {
	file, err := node.Storer.Read(req.Hash)
	if err != nil {
		return fmt.Errorf("err opening file on node %v | err: %v", node.address, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	// Send Read File Info as first message
	err = stream.Send(&proto.FileStream{
		Type: &proto.FileStream_StreamInfo{
			StreamInfo: &proto.StreamInfo{
				FileInfo: &proto.FileInfo{
					Hash:              req.Hash,
					Size:              uint64(fileInfo.Size()),
					ModificationStamp: uint64(fileInfo.ModTime().Unix()),
				},
				DataNodes: []*proto.DataNodeInfo{}, //replication param unused
			},
		},
	})
	if err != nil {
		return err
	}

	buf := make([]byte, 512*1024)
	for {
		_, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		err = stream.Send(&proto.FileStream{
			Type: &proto.FileStream_Chunk{
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
		Address:  node.address,
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
	interval := 5 * time.Second
	ticker := time.NewTicker(interval)
	success := false
	defer ticker.Stop()
	for {
		select {
		case <-node.doneChan:
			return
		case <-ticker.C:
			newInterval, err := node.SendHeartbeat()
			if err != nil {
				log.Println("hearbeat error", err)
				MAX_RETRIES -= 1
				if MAX_RETRIES <= 0 {
					node.sendError(err)
					return
				}
			}
			if !success {
				node.stateMu.Lock()
				if node.state == SafeMode {
					log.Printf("First successful heartbeat, transitioning to Active state")
					node.state = Active
				}
				node.stateMu.Unlock()
				success = true
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
	heldFiles := make([]*proto.FileInfo, len(rawFiles))
	for i, file := range rawFiles {
		fileInfo, err := file.Stat()
		if err != nil {
			return 0, err
		}
		heldFiles[i] = &proto.FileInfo{
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

func (node *DataNode) HandleIncrementalBlockreport() {
	// max consecutive retries
	MAX_RETRIES := node.MAX_RETRIES
	interval := 10 * time.Second // Default interval for initial failure case
	// Try first block report
	newInterval, err := node.SendIncrementalBlockReport()
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
			newInterval, err := node.SendIncrementalBlockReport()
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
func (node *DataNode) SendIncrementalBlockReport() (time.Duration, error) {
	// minimize lock contention
	node.cLogMu.Lock()
	logs := node.ChangeLog
	node.ChangeLog = make([]*proto.FileUpdate, 0)
	node.cLogMu.Unlock()

	c := node.NameNode.Client
	ctx := context.Background()
	resp, err := c.client.IncrementalBlockReport(ctx, &proto.IncrementalBlockReportRequest{
		NodeId:  node.Id,
		Updates: logs,
	})
	if err != nil {
		return 0, nil
	}
	return time.Duration(resp.NextReportDelay), nil

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
	node.cLogMu.Lock()
	defer node.cLogMu.Unlock()
	node.ChangeLog = append(node.ChangeLog, &proto.FileUpdate{
		FileInfo: cmd.FileInfo,
		Update:   *proto.FileUpdate_UPDATE_DELETE.Enum(),
	})
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

	fr, err := node.Storer.Read(cmd.FileInfo.Hash)
	if err != nil {
		return err
	}
	defer fr.Close()

	// Might not need this here tbh
	if len(cmd.TargetNodes) == 0 {
		log.Printf("end of replication chain reached with node %d", node.Id)
		return nil
	}

	target := cmd.TargetNodes[0]
	downstreamInfo := &proto.StreamInfo{
		FileInfo:  cmd.FileInfo,
		DataNodes: cmd.TargetNodes[1:],
	}
	ctx := context.Background()
	replicator, err := NewReplicator(ctx, target.Address, downstreamInfo)
	if err != nil {
		return err
	}

	// Then stream the file chunks
	buf := make([]byte, 512*1024) // 512KB chunks
	for {
		n, err := fr.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading file: %w", err)
		}

		err = replicator.SendChunk(&proto.FileStream{
			Type: &proto.FileStream_Chunk{
				Chunk: buf[:n],
			},
		})
		if err != nil {
			return fmt.Errorf("failed to send chunk: %w", err)
		}
	}

	response, err := replicator.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("error closing stream: %w", err)
	}
	if !response.Success {
		return fmt.Errorf("failed to replicate file: %s to node: %d", cmd.FileInfo.Hash, target)
	}

	return nil
}

func (node *DataNode) InitializeClients() error {
	// Create name node connection
	if node.NameNode.Client == nil {
		c, err := NewNameNodeClient(node.NameNode.Address)
		if err != nil {
			return err
		}
		node.NameNode.Client = c
	}
	return nil
}

func (node *DataNode) PeerRepresentation() *PeerDataNode {
	node.statMu.Lock()
	defer node.statMu.Unlock()
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
