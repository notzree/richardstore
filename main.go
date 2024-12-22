package main

import (
	"net"
	"time"

	"github.com/notzree/richardstore/proto"
)

func main() {
	addr := &net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 3000,
	}
	fsp := NewFsSnapShotStore[PersistedRaftFields]("test")
	cfg := RaftNodeConfig{
		Id:              100,
		Address:         addr,
		ElectionTimeout: 100 * time.Millisecond,
		HeartbeatPeriod: 100 * time.Millisecond,
		StateMachine:    &StorageStateMachine{Storer: Store{}},
		RaftTransport:   RaftTransport{},
		DataTransport:   DataTransport{},
		SnapshotStorer:  fsp,
		Cluster:         make([]PeerRaftNode, 0),
		logs:            make([]proto.LogEntry, 0),
	}
	raftnode := NewRaftNode(cfg)
	raftnode.Start()

}
