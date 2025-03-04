package hdfs

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/notzree/richardstore/proto"
	str "github.com/notzree/richardstore/store"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	NameNodeAddr   string
	NameNodeClient *NameNodeClient
	mu             *sync.RWMutex
	DataNodeMap    map[string]*DataNodeClient
	storer         *str.Store
}

func NewClient(nameNodeAddr string, storer *str.Store) (*Client, error) {
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

func (c *Client) WriteFile(file *os.File) (string, error) {
	return c._write(file)
}
func (c *Client) ReadFile(hash string) (*io.ReadCloser, error) {
	return c._read(hash)
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
	log.Printf("writing to %v", resp.DataNodes)
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
			log.Printf("node %d reported success", idx)
			resultCh <- writeResult{nodeIdx: idx, success: resp.Success, err: nil}
		}(i, targetNode.Address)
	}

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

func (c *Client) _read(hash string) (*io.ReadCloser, error) {
	ctx := context.Background()
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := c.NameNodeClient.client.ReadFile(ctxWithTimeout, &proto.ReadFileRequest{
		Hash: hash,
	})
	if err != nil {
		return nil, err
	}
	readerChan := make(chan io.ReadCloser, len(resp.DataNodes))
	errorChan := make(chan error, len(resp.DataNodes))
	readCtx, readCancel := context.WithCancel(context.Background())
	defer readCancel()
	for _, node := range resp.DataNodes {
		go func(nodeInfo *proto.DataNodeInfo) {
			c.mu.RLock()
			client, ok := c.DataNodeMap[nodeInfo.Address]
			if !ok {
				errorChan <- fmt.Errorf("error getting data node client for node %s", nodeInfo.Address)
			}
			c.mu.Unlock()
			ctx := context.Background()
			ctxWithTimeout, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			stream, err := client.client.ReadFile(ctxWithTimeout, &proto.ReadFileRequest{
				Hash: hash,
			})
			if err != nil {
				errorChan <- fmt.Errorf("error creating stream: %v", err)
				return
			}
			firstMsg, err := stream.Recv()
			if err != nil {
				errorChan <- fmt.Errorf("error receiving first message: %v", err)
				stream.CloseSend()
				return
			}

			if firstMsg.Request.(*proto.ReadFileStream_FileInfo).FileInfo.Hash != hash {
				errorChan <- fmt.Errorf("error hash mismatch: expected %s got %s", hash, firstMsg.Request.(*proto.ReadFileStream_FileInfo).FileInfo.Hash)
				stream.CloseSend()
				return
			}

			pr, pw := io.Pipe()
			go func() {
				defer pw.Close()
				defer stream.CloseSend()
				go func() {
					<-readCtx.Done()
					stream.CloseSend()
					pw.CloseWithError(context.Canceled)
				}()
				for {
					resp, err := stream.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						log.Printf("error receiving chunk: %v", err)
						pw.CloseWithError(fmt.Errorf("error receiving chunk: %w", err))
						return
					}
					chunk, ok := resp.Request.(*proto.ReadFileStream_Chunk)
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
			readerChan <- pr
		}(node)
	}
	var lastErr error
	remainingNodes := len(resp.DataNodes)

	for {
		select {
		case reader := <-readerChan:
			// Got a successful reader, cancel other reads and return
			readCancel()
			return &reader, nil

		case err := <-errorChan:
			remainingNodes--
			lastErr = err
			if remainingNodes == 0 {
				// All nodes failed
				return nil, fmt.Errorf("all nodes failed: %v", lastErr)
			}

		case <-ctxWithTimeout.Done():
			return nil, fmt.Errorf("read timeout")
		}
	}

}
