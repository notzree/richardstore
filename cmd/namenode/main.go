package main

import (
	"log"
	"time"

	hdfs "github.com/notzree/richardstore/hdfs"
)

func main() {
	MAXSIMCOMMANDS := 5
	nameNode := hdfs.NewNameNode(9, ":3009", nil, 2*time.Second, 30*time.Second, MAXSIMCOMMANDS)
	log.Fatal(nameNode.Run())
}
