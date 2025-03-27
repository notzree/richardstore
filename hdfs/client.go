package hdfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/notzree/richardstore/proto"
	str "github.com/notzree/richardstore/store"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	NameNodeAddr   string
	NameNodeClient *NameNodeClient
	storer         *str.Store
}

func NewClient(nameNodeAddr string, storer *str.Store) (*Client, error) {
	nameNodeClient, err := NewNameNodeClient(nameNodeAddr)
	if err != nil {
		return nil, err
	}
	client := &Client{
		NameNodeAddr: nameNodeAddr,
		storer:       storer,
	}
	client.NameNodeClient = nameNodeClient
	ctx := context.Background()
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	info, err := client.NameNodeClient.client.Info(ctxWithTimeout, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	log.Print(info)
	return client, nil
}

func (c *Client) Close() error {
	c.NameNodeClient.Close()
	return nil
}

type writeResult struct {
	nodeIdx int
	success bool
	err     error
}

func (c *Client) WriteFile(file *os.File, minRepFactor float32) (string, error) {
	return c.write(file, minRepFactor)
}
func (c *Client) ReadFile(hash string) (*io.ReadCloser, error) {
	return c.read(hash)
}

func (c *Client) write(file *os.File, minRepFactor float32) (string, error) {

	fileInfo, err := c.getFileInfo(file, minRepFactor)
	if err != nil {
		return "", err
	}
	ctx := context.Background()
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	nameNodeResp, err := c.NameNodeClient.client.WriteFile(ctxWithTimeout, &proto.WriteFileRequest{
		FileInfo: fileInfo,
	})
	if err != nil {
		return "", err
	}
	log.Print(nameNodeResp.DataNodes)
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return "", err
	}

	log.Println("initiating replication stream")

	firstPeer, err := NewDataNodeClient(nameNodeResp.DataNodes[0].Address)
	if err != nil {
		return "", err
	}
	stream, err := firstPeer.client.WriteFile(ctxWithTimeout)
	// send first message
	err = stream.Send(&proto.FileStream{
		Type: &proto.FileStream_StreamInfo{
			StreamInfo: &proto.StreamInfo{
				FileInfo:  fileInfo,
				DataNodes: nameNodeResp.DataNodes[1:],
			},
		},
	})
	buf := make([]byte, 1024)
	for {
		n, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		err = stream.Send(&proto.FileStream{
			Type: &proto.FileStream_Chunk{
				Chunk: buf[:n],
			},
		})
		if err != nil {
			return "", err
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return "", err
	}
	if !resp.Success {
		return "", errors.New("internal data node error! failed to write to datanode")
	}

	return fileInfo.Hash, nil
}

func (c *Client) read(hash string) (*io.ReadCloser, error) {
	ctx := context.Background()
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := c.NameNodeClient.client.ReadFile(ctxWithTimeout, &proto.ReadFileRequest{
		Hash: hash,
	})
	if err != nil {
		return nil, err
	}
	log.Printf("namenode instructed to read from datanodes: %v", resp.DataNodes)

	var lastErr error
	for _, node := range resp.DataNodes {
		// Try connecting to this datanode
		client, err := NewDataNodeClient(node.Address)
		if err != nil {
			log.Printf("error connecting to datanode %s: %v", node.Address, err)
			lastErr = err
			continue // Try next datanode
		}

		// Create stream
		nodeCtx := context.Background()
		nodeCtxWithTimeout, nodeCancel := context.WithTimeout(nodeCtx, 10*time.Second)

		stream, err := client.client.ReadFile(nodeCtxWithTimeout, &proto.ReadFileRequest{
			Hash: hash,
		})
		if err != nil {
			log.Printf("error creating stream with datanode %s: %v", node.Address, err)
			nodeCancel()
			lastErr = err
			continue // Try next datanode
		}

		// Check first message
		firstMsg, err := stream.Recv()
		if err != nil {
			log.Printf("error receiving first message from datanode %s: %v", node.Address, err)
			stream.CloseSend()
			nodeCancel()
			lastErr = err
			continue // Try next datanode
		}

		// Verify hash
		if firstMsg.Type.(*proto.FileStream_StreamInfo).StreamInfo.FileInfo.Hash != hash {
			log.Printf("hash mismatch from datanode %s: expected %s got %s",
				node.Address, hash, firstMsg.Type.(*proto.FileStream_StreamInfo).StreamInfo.FileInfo.Hash)
			stream.CloseSend()
			nodeCancel()
			lastErr = fmt.Errorf("hash mismatch")
			continue // Try next datanode
		}

		// This datanode looks good, create pipe and start reading
		pr, pw := io.Pipe()

		// Start background goroutine to handle reading from stream and writing to pipe
		go func() {
			defer pw.Close()
			defer stream.CloseSend()
			defer nodeCancel()

			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					// Normal end of stream
					break
				}
				if err != nil {
					log.Printf("error receiving chunk: %v", err)
					pw.CloseWithError(fmt.Errorf("error receiving chunk: %w", err))
					return
				}

				chunk, ok := resp.Type.(*proto.FileStream_Chunk)
				if !ok {
					pw.CloseWithError(fmt.Errorf("expected chunk message"))
					log.Printf("error: expected chunk message")
					return
				}

				_, err = pw.Write(chunk.Chunk)
				if err != nil {
					pw.CloseWithError(fmt.Errorf("error writing to pipe: %w", err))
					log.Printf("error writing chunk: %v", err)
					return
				}
			}
		}()

		// Successfully set up reader with this datanode
		reader := io.ReadCloser(pr)
		return &reader, nil
	}

	// If we get here, all datanodes failed
	return nil, fmt.Errorf("all datanodes failed, last error: %v", lastErr)
}

func (c *Client) getFileInfo(file *os.File, minRepFactor float32) (*proto.FileInfo, error) {
	// Get file information
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// Calculate a simple hash from the file path and modification time
	expectedFileAddr, err := c.storer.CreateAddress(file)
	if err != nil {
		return nil, err
	}

	// Create the FileInfo protobuf message
	pbFileInfo := &proto.FileInfo{
		Hash:                 expectedFileAddr.HashStr,
		Size:                 uint64(fileInfo.Size()),
		MinReplicationFactor: minRepFactor, // Default value, adjust as needed
		ModificationStamp:    uint64(fileInfo.ModTime().UnixNano()),
		GenerationStamp:      uint64(time.Now().UnixNano()), // Current time as per requirements
	}

	return pbFileInfo, nil
}
