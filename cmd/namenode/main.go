package main

import (
	"log"
	"time"

	hdfs "github.com/notzree/richardstore/hdfs"
)

func main() {
	MAXSIMCOMMANDS := 5
	namenode := hdfs.NewNameNode(uint64(0), ":3009", 5*time.Second, 1*time.Minute, MAXSIMCOMMANDS)
	log.Fatal(namenode.Run())
}
