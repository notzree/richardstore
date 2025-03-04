package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/notzree/richardstore/hdfs"
	"github.com/notzree/richardstore/store"
)

func main() {
	// MAXSIMCOMMANDS := 5
	// NUMNODES := 5
	const (
		KB          = 1024
		MB          = 1024 * KB
		sizeInBytes = 50 * MB
	)
	namenode := hdfs.NewNameNode(0, ":3009", 5*time.Second, 1*time.Minute, 20)
	go namenode.Run()

	for i := 1; i <= 5; i++ {
		addr := fmt.Sprintf(":300%v", i)
		storer := store.NewStore(store.StoreOpts{BlockSize: 5, Root: addr})
		datanode := hdfs.NewDataNode(uint64(i), addr, storer, 20, 5, nil)
		datanode.NameNode = hdfs.PeerNameNode{
			Id:      0,
			Address: ":3009",
			Client:  nil,
		}
		go datanode.Run()
	}
	storer := store.NewStore(store.StoreOpts{BlockSize: 5, Root: "root"})
	client, err := hdfs.NewClient(":3009", storer)
	if err != nil {
		log.Fatal(err)
	}
	file, err := os.Open("test")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	time.Sleep(1 * time.Second)
	client.WriteFile(file)
	time.Sleep(10 * time.Second)
}
