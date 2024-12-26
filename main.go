package main

import (
	"fmt"
	"io"
	"log"
	"net"
)

func main() {
	addr := &net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 3000,
	}
	// fsp := NewFsSnapShotStore[PersistedRaftFields]("test")
	// cfg := RaftNodeConfig{
	// 	Id:              100,
	// 	Address:         addr,
	// 	ElectionTimeout: 100 * time.Millisecond,
	// 	HeartbeatPeriod: 100 * time.Millisecond,
	// 	StateMachine:    &StorageStateMachine{Storer: Store{}},
	// 	RaftTransport:   &RaftTransport{},
	// 	DataManager:     &DataManager{},
	// 	SnapshotStorer:  fsp,
	// 	Cluster:         make([]PeerRaftNode, 0),
	// 	logs:            make([]proto.LogEntry, 0),
	// }
	// raftnode := NewRaftNode(cfg)
	// raftnode.Start()
	tr := NewDataTransport(addr)
	go func() {
		for {
			select {
			case tcpData := <-tr.Consume():
				data, err := io.ReadAll(tcpData.r)
				if err != nil {
					log.Println(err)
				}
				// fmt.Printf("%s", data)
				fmt.Println(string(data))
				tcpData.wg.Done()
			}
		}
	}()
	tr.Start()
}
