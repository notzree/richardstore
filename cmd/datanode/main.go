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
	if replicaNum == "" {
		replicaNum = "0"
	}
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

	total, err := strconv.Atoi(totalReplicas)
	if err != nil {
		log.Fatalf("Invalid total replicas: %v", err)
	}

	datanode := hdfs.NewDataNode(uint64(offset), ":"+port, s.NewStore(s.StoreOpts{
		BlockSize: 5,
		Root:      ":" + port,
	}), MAXSIMCOMMANDS, 5, 0)
	PeerNodes := make([]hdfs.PeerDataNode, total)
	for i := 0; i < (total); i++ {
		if i == offset {
			continue
		}
		address := ":" + strconv.Itoa(basePort+i)
		PeerNodes[i] = hdfs.PeerDataNode{
			Id:       uint64(i + 1),
			Address:  address,
			Alive:    true,
			Capacity: 0,
			Used:     0,
		}
	}
	datanode.AddDataNodes(PeerNodes)
	datanode.NameNode = hdfs.PeerNameNode{
		Id:      9,
		Address: nameNodeAddr,
		Client:  nil,
	}

	log.Fatal(datanode.Run())
}
