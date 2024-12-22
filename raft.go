package main

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	"github.com/notzree/richardstore/proto"
	pr "google.golang.org/protobuf/proto"
)

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

type ApplyResult struct {
	Result []byte
	Error  error
}

type SpecifiedCommand interface {
	Serialize(*proto.Command) error
}

func SerializeCommand(specified_cmd SpecifiedCommand, unspecific_cmd *proto.Command) error {
	var cmd *proto.Command
	if err := specified_cmd.Serialize(cmd); err != nil {
		return err
	}
	return nil
}

func DeserializeCommand[T pr.Message](cmd *proto.Command, dest T) error {
	return pr.Unmarshal(cmd.Data, dest)
}

type StateMachine interface {
	Apply(cmd *proto.Command) (*ApplyResult, error) // maybe be io reader instead of slice of bytes
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []proto.LogEntry
	LeaderCommit uint64
}

// PeerRaftNode represents what a node in Raft sees about other neighbour nodes
type PeerRaftNode struct {
	Id uint64
	// The address that all RPCs are sent to
	RPCAddress net.Addr
	// The address that should be used to transfer data (files)
	DataTransferAddress net.Addr
	// index of next log entry to send
	nextIndex uint64
	// index of highest log entry known to be replicated
	matchIndex uint64
	// who was voted for in most recent term
	votedFor uint64
}

// Fields to persist excluding Logs
type PersistedRaftFields struct {
	CurrentTerm uint64
	// Log         []proto.LogEntry // Probably want to separate persisting logs vs metadata
	VotedFor uint64
}

// RaftNode represents a node in a Raft cluster
type RaftNode struct {
	// these are for gRPC

	// These variables for shutting down.
	done bool

	Debug bool

	mu sync.Mutex
	// ----------- PERSISTENT STATE -----------
	PersistedRaftFields

	// Separating logs from rest of the fields b/c persist differntly
	logs []proto.LogEntry

	// ----------- READONLY STATE -----------

	// Unique identifier for this Server
	id uint64

	// Address for RPC
	address net.Addr

	// When to start elections after no append entry messages
	electionTimeout time.Duration

	// How often to send empty messages
	hearbeatPeriod time.Duration

	// When to next send empty message
	heartbeatTimeout time.Time

	// User-provided state machine
	statemachine StateMachine

	// Transport for communicating between nodes
	raftTransport RaftTransport

	// Transport for sending large amounts of data over wire
	dataTransport DataTransport

	logStorer      SnapshotStorer[[]proto.LogEntry] // TODO: In the future maybe need to make this more efficient
	metadataStorer SnapshotStorer[PersistedRaftFields]
	// // Metadata directory
	// metadataDir string

	// // Metadata store, should probably make this into a storer interface or smth
	// fd *os.File

	// ----------- VOLATILE STATE -----------

	// Index of highest log entry known to be committed
	commitIndex uint64

	// Index of highest log entry applied to state machine
	lastApplied uint64

	// Candidate, follower, or leader
	state ServerState

	// Peer Raft nodes
	peers []PeerRaftNode
}

type RaftNodeConfig struct {
	Id              uint64
	Address         net.Addr
	ElectionTimeout time.Duration
	HeartbeatPeriod time.Duration
	StateMachine    StateMachine
	RaftTransport   RaftTransport
	DataTransport   DataTransport
	SnapshotStorer  SnapshotStorer[PersistedRaftFields]
	Cluster         []PeerRaftNode
	logs            []proto.LogEntry
}

func NewRaftNode(cfg RaftNodeConfig) *RaftNode {
	var peers []PeerRaftNode
	for _, c := range cfg.Cluster {
		if c.Id == 0 {
			panic("peer id not allowed to be zero")
		}
		peers = append(peers, c)
	}
	logs := cfg.logs
	if logs == nil {
		logs = make([]proto.LogEntry, 0)
	}

	return &RaftNode{
		done: false,
		PersistedRaftFields: PersistedRaftFields{
			CurrentTerm: 0,
			VotedFor:    0,
		},
		logs:             logs,
		id:               cfg.Id,
		address:          cfg.Address,
		electionTimeout:  cfg.ElectionTimeout,
		hearbeatPeriod:   cfg.HeartbeatPeriod,
		heartbeatTimeout: time.Now().Add(cfg.HeartbeatPeriod),
		statemachine:     cfg.StateMachine,
		raftTransport:    cfg.RaftTransport,
		dataTransport:    cfg.DataTransport,
		metadataStorer:   cfg.SnapshotStorer,
		commitIndex:      0,
		lastApplied:      0,
		peers:            peers,
		state:            Follower, // Initial state

	}
}

func (node *RaftNode) Stop() {

}

func (node *RaftNode) Start() {
	defer node.Stop()
	go func() {
		log.Fatal(RunRaftServer(node, node.address))
	}()
	// go func () {
	// log.Fatal(node.dataTransport.Listen())
	// }()

}

// Persists currentTerm + log to metadata storage
func (node *RaftNode) persist() error {
	if err := node.logStorer.Persist(node.logs); err != nil {
		return err
	}
	if err := node.metadataStorer.Persist(node.PersistedRaftFields); err != nil {
		return err
	}
	return nil
}

func (node *RaftNode) HandleAppendEntriesRequest(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	// term < currentTerm
	if req.Term < node.CurrentTerm || req.PrevLogIndex > uint64(len(node.logs)-1) {
		return &proto.AppendEntriesResponse{
			Term:    node.CurrentTerm,
			Success: false,
		}, nil
	}

	if req.Term > node.CurrentTerm {
		node.CurrentTerm = req.Term
		node.VotedFor = 0 // or however you represent "no vote"
		node.state = Follower
	}
	node.resetElectionTimer() // or however you handle election timeouts

	// log does not contain an entry at prevLogIndex whose term matches prevLogTerm
	prevLog := &node.logs[req.PrevLogIndex]
	if prevLog.Term != req.PrevLogTerm {
		return &proto.AppendEntriesResponse{
			Term:    node.CurrentTerm,
			Success: false,
		}, nil
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	newEntries := make([]proto.LogEntry, 0) // Changed to non-pointer slice
	for i, entry := range req.Entries {
		if entry.Index < uint64(len(node.logs)) {
			if node.logs[entry.Index].Term != entry.Term {
				// conflict found, truncate logs from this point
				node.logs = node.logs[:entry.Index]
				// Convert pointer entries to values
				for _, e := range req.Entries[i:] {
					newEntries = append(newEntries, proto.LogEntry{
						Term:  e.Term,
						Index: e.Index,
						Cmd:   e.Cmd,
					})
				}
				break
			}
		} else {
			// reached end of existing log. Append remaining entries
			for _, e := range req.Entries[i:] {
				newEntries = append(newEntries, proto.LogEntry{
					Term:  e.Term,
					Index: e.Index,
					Cmd:   e.Cmd,
				})
			}
			break
		}
	}
	// append new logs
	node.logs = append(node.logs, newEntries...)

	//
	if req.LeaderCommit > node.commitIndex {
		node.commitIndex = min(req.LeaderCommit, uint64(len(node.logs)-1))
	}
	return &proto.AppendEntriesResponse{
		Term:    node.CurrentTerm,
		Success: true,
	}, nil
}

// func (node *RaftNode) handleCommand(cmd *proto.Command) error {
// 	res, err := node.statemachine.Apply(cmd)
// 	if err != nil {
// 		return err
// 	}
// 	// TODO: implement this
// }

func (node *RaftNode) resetElectionTimer() {
	//TODO: Implement this

}

//TODO:

// What if instead, each server exposes a fixed datatransfer port.

// Then, when the leader decides to append a write command, it will go ahead and send that file over to the other servers via a TCP connection on the data transfer port. Since I'm using a CAS file system, the incoming files can be read and stored. Then in the writecmd, the hash for each file can be checked. The nodes will maintain an internal list of files that have not been confirmed (This could be reasons like Consensus was not reached, but transfer still happened). Then, after each term, the nodes will periodically clear our the orphaned files.

// This would sort of mean that the data transfer is taking place asynchronously from the log propogation.

func min[T ~int | ~uint64](a, b T) T {
	if a < b {
		return a
	}
	return b
}
