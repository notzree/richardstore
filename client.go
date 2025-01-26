package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/notzree/richardstore/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	NameNodeAddr   string
	NameNodeClient *NameNodeClient
	mu             *sync.RWMutex
	DataNodeMap    map[string]*DataNodeClient
	storer         *Store
}

func NewClient(nameNodeAddr string, storer *Store) (*Client, error) {
	nameNodeClient, err := NewNameNodeClient(nameNodeAddr)
	if err != nil {
		return nil, err
	}
	client := &Client{
		NameNodeAddr: nameNodeAddr,
		mu:           &sync.RWMutex{},
		DataNodeMap:  make(map[string]*DataNodeClient),
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

	for _, peerInfo := range info.DataNodes {
		peerClient, err := NewDataNodeClient(peerInfo.Address)
		if err != nil {
			return nil, err
		}
		client.DataNodeMap[peerInfo.Address] = peerClient
	}
	return client, nil
}

func (c *Client) Close() error {
	for _, peerClient := range c.DataNodeMap {
		peerClient.Close()
	}
	c.NameNodeClient.Close()
	return nil
}

type writeResult struct {
	nodeIdx int
	success bool
	err     error
}

func (c *Client) _write(file *os.File) (string, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("failed to get file info: %w", err)
	}
	size := uint64(fileInfo.Size())

	ctx := context.Background()
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	resp, err := c.NameNodeClient.client.CreateFile(ctxWithTimeout, &proto.CreateFileRequest{
		Size:                 size,
		MinReplicationFactor: 0.5,
	})
	log.Printf("data nodes: %s", resp.DataNodes)
	if err != nil {
		return "", err
	}

	expectedFileAddr, err := c.storer.CreateAddress(file)
	if err != nil {
		return "", err
	}

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return "", err
	}

	resultCh := make(chan writeResult, len(resp.DataNodes))
	var wg sync.WaitGroup

	for i, targetNode := range resp.DataNodes {
		wg.Add(1)
		go func(idx int, addr string) {
			defer wg.Done()

			c.mu.RLock()
			peerClient, ok := c.DataNodeMap[addr]
			c.mu.RUnlock()

			if !ok {
				resultCh <- writeResult{nodeIdx: idx, success: false, err: fmt.Errorf("node not found: %s", addr)}
				return
			}

			stream, err := peerClient.client.WriteFile(ctxWithTimeout)
			if err != nil {
				resultCh <- writeResult{nodeIdx: idx, success: false, err: fmt.Errorf("failed to create stream: %w", err)}
				return
			}

			err = stream.Send(&proto.WriteFileRequest{
				Request: &proto.WriteFileRequest_FileInfo{
					FileInfo: &proto.FileInfo{
						Hash: expectedFileAddr.HashStr,
						Size: size,
					},
				},
			})
			if err != nil {
				resultCh <- writeResult{nodeIdx: idx, success: false, err: fmt.Errorf("failed to send file info: %w", err)}
				return
			}
			log.Printf("sending first req")
			buf := make([]byte, 1024)
			for {
				n, err := file.Read(buf)
				if err == io.EOF {
					break
				}
				if err != nil {
					resultCh <- writeResult{nodeIdx: idx, success: false, err: fmt.Errorf("error reading file: %w", err)}
					return
				}

				err = stream.Send(&proto.WriteFileRequest{
					Request: &proto.WriteFileRequest_Chunk{
						Chunk: buf[:n],
					},
				})
				if err != nil {
					resultCh <- writeResult{nodeIdx: idx, success: false, err: fmt.Errorf("failed to send chunk: %w", err)}
					return
				}
			}

			resp, err := stream.CloseAndRecv()
			if err != nil {
				resultCh <- writeResult{nodeIdx: idx, success: false, err: fmt.Errorf("error receiving response: %w", err)}
				return
			}

			resultCh <- writeResult{nodeIdx: idx, success: resp.Success, err: nil}
		}(i, targetNode.Address)
	}

	// Close result channel once all goroutines complete
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect results and check for failures
	var errors []error
	successCount := 0
	for result := range resultCh {
		if result.err != nil {
			errors = append(errors, fmt.Errorf("node %d: %w", result.nodeIdx, result.err))
		} else if result.success {
			successCount++
		}
	}

	if len(errors) > 0 {
		return "", fmt.Errorf("write failed on some nodes: %v", errors)
	}

	if successCount == 0 {
		return "", fmt.Errorf("write failed on all nodes")
	}

	return expectedFileAddr.HashStr, nil
}

//TODO: IMPLEMENT THE INFO RPC METHOD FOR THE NAME NODE.
