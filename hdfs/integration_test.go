package hdfs

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/notzree/richardstore/proto"
	"github.com/notzree/richardstore/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	nameNodePort  = "127.0.0.1:9000"
	dataNode1Port = "127.0.0.1:9001"
	dataNode2Port = "127.0.0.1:9002"
	dataNode3Port = "127.0.0.1:9003"

	testFileContent = "This is a test file content for integration testing"

	// Config parameters
	maxSimCommands                 = 10
	heartbeatInterval              = 1 * time.Second
	blockReportInterval            = 5 * time.Second
	incrementalBlockReportInterval = 5 * time.Second
	maxRetries                     = 3
)

// Helper to create a temporary test directory
func createTempDir(t *testing.T, prefix string) string {
	dir, err := os.MkdirTemp("", prefix)
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}

// Helper to create a test file with content
func createTestFile(t *testing.T, dir, content string) (string, string) {
	filePath := filepath.Join(dir, "testfile.txt")
	err := os.WriteFile(filePath, []byte(content), 0644)
	require.NoError(t, err)

	// Calculate hash based on your hashing method
	// This is a placeholder, adjust based on actual implementation
	hash := fmt.Sprintf("%x", []byte(content))

	return filePath, hash
}

// Creates a Client for the NameNode
func createNameNodeClient(t *testing.T) *NameNodeClient {
	client, err := NewNameNodeClient(nameNodePort)
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
	})
	return client
}

// Creates a Client for a DataNode
func createDataNodeClient(t *testing.T, address string) *DataNodeClient {
	client, err := NewDataNodeClient(address)
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
	})
	return client
}

// Creates an HDFS Client
func createHDFSClient(t *testing.T) *Client {
	storeDir := createTempDir(t, "client-store")
	storer := store.NewStore(store.StoreOpts{
		Root:      storeDir,
		Capacity:  1024 * 1024 * 10, // 10MB
		BlockSize: 5,
	})

	client, err := NewClient(nameNodePort, storer)
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
	})
	return client
}

// Setup and run a NameNode for testing
func setupNameNode(t *testing.T) *NameNode {
	nameNode := NewNameNode(
		1,
		nameNodePort,
		heartbeatInterval,
		blockReportInterval,
		incrementalBlockReportInterval,
		maxSimCommands,
	)

	// Start in a goroutine so it doesn't block
	go func() {
		err := nameNode.Run()
		if err != nil {
			log.Printf("NameNode stopped with error: %v", err)
		}
	}()

	// Wait for the server to start
	time.Sleep(500 * time.Millisecond)

	return nameNode
}

// Setup and run a DataNode for testing
func setupDataNode(t *testing.T, id uint64, address string, nameNodeAddr string) *DataNode {
	storeDir := createTempDir(t, fmt.Sprintf("datanode-%d-", id))

	// Create a store with appropriate capacity
	storer := store.NewStore(store.StoreOpts{
		Root:      storeDir,
		Capacity:  1024 * 1024 * 100,
		BlockSize: 5,
	}) // 100MB capacity

	dataNode := NewDataNode(id, address, storer, maxSimCommands, maxRetries)

	// Set the NameNode connection
	dataNode.NameNode = PeerNameNode{
		Id:      1,
		Address: nameNodeAddr,
	}

	// Start in a goroutine
	go func() {
		err := dataNode.Run()
		if err != nil {
			log.Printf("DataNode %d stopped with error: %v", id, err)
		}
	}()

	// Wait for it to initialize
	time.Sleep(heartbeatInterval)

	return dataNode
}

// Helper to write a file to HDFS using the Client API
func writeFileToHDFSUsingClient(t *testing.T, client *Client, content string, minReplication float32) string {
	tempDir := createTempDir(t, "client-write")
	filePath := filepath.Join(tempDir, "temp.txt")

	// Create temporary file with content
	err := os.WriteFile(filePath, []byte(content), 0644)
	require.NoError(t, err)

	// Open the file for reading
	file, err := os.Open(filePath)
	require.NoError(t, err)
	defer file.Close()

	// Write to HDFS using client
	hash, err := client.WriteFile(file, minReplication)
	require.NoError(t, err)

	return hash
}

// Helper to read a file from HDFS using the Client API
func readFileFromHDFSUsingClient(t *testing.T, client *Client, hash string) string {
	reader, err := client.ReadFile(hash)
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)

	return string(data)
}

// TestBasicNodeIntegration tests basic NameNode and DataNode integration
func TestBasicNodeIntegration(t *testing.T) {
	// Setup NameNode
	nameNode := setupNameNode(t)
	defer nameNode.Stop()

	// Setup 3 DataNodes
	dn1 := setupDataNode(t, 101, dataNode1Port, nameNodePort)
	dn2 := setupDataNode(t, 102, dataNode2Port, nameNodePort)
	dn3 := setupDataNode(t, 103, dataNode3Port, nameNodePort)
	defer dn1.Stop()
	defer dn2.Stop()
	defer dn3.Stop()

	// Wait for heartbeat interval to pass
	time.Sleep(heartbeatInterval * 2)

	// Verify DataNodes are registered
	nameNode.dnMu.RLock()
	assert.Len(t, nameNode.DataNodes, 3, "Not all DataNodes registered")
	nameNode.dnMu.RUnlock()

	// Create HDFS client
	client := createHDFSClient(t)

	// Write a file with replication factor 2
	content := testFileContent
	hash := writeFileToHDFSUsingClient(t, client, content, 0.7) // 2/3 of the nodes

	// Wait for block reports to propogate back to name node
	time.Sleep(blockReportInterval)

	// Verify file is recorded in NameNode's FileMap
	fileEntry := nameNode.Fmp.Has(hash)
	assert.NotNil(t, fileEntry, "File not registered in NameNode")
	assert.Equal(t, uint64(len(content)), fileEntry.Size, "File size mismatch")

	// Verify it's replicated to at least 2 DataNodes
	assert.GreaterOrEqual(t, len(fileEntry.Replicas), 2, "File not replicated to enough DataNodes")

	// Read the file back
	readContent := readFileFromHDFSUsingClient(t, client, hash)
	assert.Equal(t, content, readContent, "Read content doesn't match written content")

	// Delete the file using NameNode client
	nnClient := createNameNodeClient(t)
	deleteResp, err := nnClient.client.DeleteFile(context.Background(), &proto.DeleteFileRequest{
		Hash: hash,
	})
	require.NoError(t, err)
	assert.True(t, deleteResp.Success, "Delete operation not successful")

	// Wait for delete commands to propagate to DataNodes
	time.Sleep(heartbeatInterval * 2)

	// Verify file is deleted from NameNode's FileMap
	fileEntry = nameNode.Fmp.Has(hash)
	assert.Nil(t, fileEntry, "File not deleted from NameNode")

	// Verify file is deleted from DataNodes by attempting to read it
	// This should fail or return an error
	_, err = client.ReadFile(hash)
	assert.Error(t, err, "File should not be available after deletion")
}

// TestCommandPropagation tests that commands from NameNode to DataNodes are properly propagated
func TestCommandPropagation(t *testing.T) {
	// Setup NameNode
	nameNode := setupNameNode(t)
	defer nameNode.Stop()

	// Setup 3 DataNodes
	dn1 := setupDataNode(t, 101, dataNode1Port, nameNodePort)
	dn2 := setupDataNode(t, 102, dataNode2Port, nameNodePort)
	dn3 := setupDataNode(t, 103, dataNode3Port, nameNodePort)
	defer dn1.Stop()
	defer dn2.Stop()
	defer dn3.Stop()

	// Wait for DataNodes to register with NameNode
	time.Sleep(heartbeatInterval * 3)

	// Create HDFS client
	client := createHDFSClient(t)

	// Write a file with replication factor 1 (to first DataNode only)
	content := testFileContent
	hash := writeFileToHDFSUsingClient(t, client, content, 0.34) // 1/3 of the nodes

	// Wait for block reports to propagate
	time.Sleep(blockReportInterval)

	// Verify file is on only one DataNode
	fileEntry := nameNode.Fmp.Has(hash)
	require.NotNil(t, fileEntry, "File not registered in NameNode")
	assert.Equal(t, 1, len(fileEntry.Replicas), "File should be on exactly one DataNode")

	// Get the node ID that has the file
	var sourceNodeID uint64
	for id := range fileEntry.Replicas {
		sourceNodeID = id
		break
	}

	// Get the target node ID (the one that doesn't have the file)
	targetNodeID := uint64(101)
	if sourceNodeID == 101 {
		targetNodeID = 102
	}
	var addr string
	if targetNodeID == 101 {
		addr = dataNode1Port
	} else {
		addr = dataNode2Port
	}

	// Create a replication command manually
	nameNode.cmdMu.Lock()
	nameNode.Commands[sourceNodeID] = append(nameNode.Commands[sourceNodeID], &proto.Command{
		Command: &proto.Command_Replicate{
			Replicate: &proto.ReplicateCommand{
				FileInfo: fileEntry.FileInfo,
				TargetNodes: []*proto.DataNodeInfo{
					{
						Id:      targetNodeID,
						Address: addr,
					},
				},
			},
		},
	})
	nameNode.cmdMu.Unlock()

	// Wait for heartbeat to deliver the command and for replication to complete
	time.Sleep(blockReportInterval)

	// Verify file is now on both DataNodes
	fileEntry = nameNode.Fmp.Has(hash)
	require.NotNil(t, fileEntry, "File not found in NameNode after replication")
	assert.Equal(t, 2, len(fileEntry.Replicas), "File should be on both DataNodes after replication")

	// Read the file back to verify replication worked
	readContent := readFileFromHDFSUsingClient(t, client, hash)
	assert.Equal(t, content, readContent, "Read content doesn't match written content after replication")
}

// TestFailureRecovery tests system behavior when a DataNode fails and comes back
func TestFailureRecovery(t *testing.T) {
	// Setup NameNode
	nameNode := setupNameNode(t)
	defer nameNode.Stop()

	// Setup 2 DataNodes
	dn1 := setupDataNode(t, 101, dataNode1Port, nameNodePort)
	dn2 := setupDataNode(t, 102, dataNode2Port, nameNodePort)
	defer dn1.Stop()
	defer dn2.Stop()

	// Wait for DataNodes to register with NameNode
	time.Sleep(heartbeatInterval * 3)

	// Create HDFS client
	client := createHDFSClient(t)

	// Write a file with replication factor 1
	content := testFileContent
	hash := writeFileToHDFSUsingClient(t, client, content, 1.0) // Ensure it goes to all nodes

	// Wait for block reports to propagate
	time.Sleep(blockReportInterval)

	// Verify file is on both DataNodes
	fileEntry := nameNode.Fmp.Has(hash)
	require.NotNil(t, fileEntry, "File not registered in NameNode")
	assert.Equal(t, 2, len(fileEntry.Replicas), "File should be on both DataNodes")
	// Simulate DataNode 1 failure by changing last seen
	nameNode.dnMu.Lock()
	dataNode := nameNode.DataNodes[101]
	dataNode.LastSeen = time.Now().Add(-1 * time.Hour) // Last seen a long time ago
	nameNode.dnMu.Unlock()
	// Try to read the file - should still work because DataNode 2 has it
	readContent := readFileFromHDFSUsingClient(t, client, hash)
	assert.Equal(t, content, readContent, "Read content doesn't match written content after node failure")

	// Now "recover" DataNode 1
	nameNode.dnMu.Lock()
	dataNode = nameNode.DataNodes[101]
	dataNode.LastSeen = time.Now()
	nameNode.dnMu.Unlock()

	// The DataNode should send a block report soon
	time.Sleep(blockReportInterval * 2)

	// Verify file is still on both DataNodes
	fileEntry = nameNode.Fmp.Has(hash)
	require.NotNil(t, fileEntry, "File not registered in NameNode after recovery")
	assert.Equal(t, 2, len(fileEntry.Replicas), "File should still be on both DataNodes after recovery")

	// Read file again to verify everything still works
	readContent = readFileFromHDFSUsingClient(t, client, hash)
	assert.Equal(t, content, readContent, "Read content doesn't match written content after node recovery")
}
