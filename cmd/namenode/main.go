package main

import (
	"log"
	"os"
	"time"

	hdfs "github.com/notzree/richardstore/hdfs"
)

func main() {
	// Configuration constants
	const MAXSIMCOMMANDS = 5

	// Get port from environment or use default
	port := os.Getenv("NAMENODE_PORT")
	if port == "" {
		port = "3009"
	}

	// Configure intervals
	heartbeatInterval := 5 * time.Second
	blockReportInterval := 1 * time.Minute
	incrBlockReportInterval := 30 * time.Second

	// Use persistent volume path if available
	dataRoot := "/data"
	if _, err := os.Stat("/data"); os.IsNotExist(err) {
		// Fallback for local development
		dataRoot = "namenode-data"
		// Ensure the directory exists
		os.MkdirAll(dataRoot, 0755)
	}
	log.Printf("Using data root: %s", dataRoot)

	// Create and start the NameNode
	listenAddress := ":" + port
	log.Printf("Starting NameNode on %s", listenAddress)
	log.Printf("Heartbeat interval: %v, Block report interval: %v", heartbeatInterval, blockReportInterval)

	namenode := hdfs.NewNameNode(
		uint64(0),
		listenAddress,
		heartbeatInterval,
		blockReportInterval,
		incrBlockReportInterval,
		MAXSIMCOMMANDS,
	)

	log.Fatal(namenode.Run())
}
