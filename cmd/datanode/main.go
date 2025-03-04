package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

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

	// Get replica number from environment
	replicaNum := os.Getenv("REPLICA_NUMBER")
	fmt.Println(replicaNum)
	if replicaNum == "" {
		replicaNum = "0"
	}
	fmt.Println(replicaNum)
	totalReplicas := os.Getenv("TOTAL_REPLICAS")
	if totalReplicas == "" {
		totalReplicas = "1"
	}
	nameNodeAddr := os.Getenv("NAMENODE_ADDRESS")
	if nameNodeAddr == "" {
		nameNodeAddr = ":3009"
	}

	// Convert to int and calculate port
	offset, err := strconv.Atoi(replicaNum)
	if err != nil {
		log.Fatalf("Invalid replica number: %v", err)
	}
	port := fmt.Sprintf("%d", basePort+offset)

	if err != nil {
		log.Fatalf("Invalid total replicas: %v", err)
	}

	datanode := hdfs.NewDataNode(uint64(offset), ":"+port, s.NewStore(s.StoreOpts{
		BlockSize: 5,
		Root:      ":" + port,
	}), MAXSIMCOMMANDS, 5, nil)
	datanode.NameNode = hdfs.PeerNameNode{
		Id:      9,
		Address: nameNodeAddr,
		Client:  nil,
	}

	log.Fatal(datanode.Run())
}
