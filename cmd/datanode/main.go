package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	hdfs "github.com/notzree/richardstore/hdfs"
	s "github.com/notzree/richardstore/store"
)

func main() {
	const (
		KB             = 1024
		MB             = 1024 * KB
		sizeInBytes    = 50 * MB
		basePort       = 3000
		MAXSIMCOMMANDS = 5
	)

	// Get pod name from Kubernetes environment
	podName := os.Getenv("POD_NAME")
	log.Printf("Pod name: %s", podName)

	// Extract replica number from pod name (datanode-X)
	replicaNum := "0" // Default
	if podName != "" {
		parts := strings.Split(podName, "-")
		if len(parts) > 1 {
			replicaNum = parts[len(parts)-1]
		}
	} else {
		// Fallback if POD_NAME isn't set (local development)
		replicaNum = os.Getenv("REPLICA_NUMBER")
		if replicaNum == "" {
			replicaNum = "0"
		}
	}
	log.Printf("Using replica number: %s", replicaNum)

	// Get NameNode address
	nameNodeAddr := os.Getenv("NAMENODE_ADDRESS")
	if nameNodeAddr == "" {
		nameNodeAddr = ":3009"
	}
	log.Printf("Using NameNode address: %s", nameNodeAddr)

	// Convert to int and calculate port
	offset, err := strconv.Atoi(replicaNum)
	if err != nil {
		log.Fatalf("Invalid replica number %s: %v", replicaNum, err)
	}

	port := fmt.Sprintf("%d", basePort+offset)
	log.Printf("DataNode listening on port: %s", port)

	// Use persistent volume path if available
	dataRoot := "/data/node-" + replicaNum
	if _, err := os.Stat("/data"); os.IsNotExist(err) {
		// Fallback for local development
		dataRoot = "data-" + replicaNum
		// Ensure the directory exists
		os.MkdirAll(dataRoot, 0755)
	}
	log.Printf("Using data root: %s", dataRoot)

	var nodeAddress string
	if podName != "" {
		// In Kubernetes, use the full DNS name
		nodeAddress = fmt.Sprintf("%s.datanode.default.svc.cluster.local:%s", podName, port)
		log.Printf("Running in Kubernetes, using address: %s", nodeAddress)
	} else {
		// Fallback for local development
		nodeAddress = ":" + port
		log.Printf("Running locally, using address: %s", nodeAddress)
	}
	datanode := hdfs.NewDataNode(uint64(offset), nodeAddress, s.NewStore(s.StoreOpts{
		BlockSize: 5,
		Root:      dataRoot,
		Capacity:  0,
	}), MAXSIMCOMMANDS, 5)

	// Set NameNode connection
	datanode.NameNode = hdfs.PeerNameNode{
		Id:      9,
		Address: nameNodeAddr,
		Client:  nil,
	}

	// Run the DataNode
	log.Printf("Starting DataNode %d connected to NameNode at %s", offset, nameNodeAddr)
	log.Fatal(datanode.Run())
}
