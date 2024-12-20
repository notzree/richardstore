package main

import (
	"net"
	"sync"
	"time"

	"github.com/notzree/richardstore/proto"
)

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

type CommandType int

const (
	CommandWrite CommandType = iota
	CommandDelete
)

type Command struct {
	Type CommandType
	Data []byte // serialized data (hash, port to listen on, etc)
}

type LogEntry struct {
	// Term when entry was received by Leader
	Term uint64

	// Index in the log
	Index uint64

	// Command to execute (etcd style byte array)
	Cmd Command
}

type ApplyResult struct {
	Result []byte
	Error  error
}

type StateMachine interface {
	Apply(cmd Command) ApplyResult // maybe be io reader instead of slice of bytes
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type RaftTransport interface {
	// TODO: add methods
}
type Datatransport interface {
	// TOOD: add methods
}

// PeerRaftNode represents what a node in Raft sees about other neighbour nodes
type PeerRaftNode struct {
	Id      uint64
	Address net.Addr
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
	electionTimeout time.Time

	// How often to send empty messages
	hearbeatPeriod time.Duration

	// When to next send empty message
	heartbeatTimeout time.Time

	// User-provided state machine
	statemachine StateMachine

	// Transport for communicating between nodes
	raftTransport RaftTransport

	// Transport for sending large amounts of data over wire
	dataTransport Datatransport

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
	ElectionTimeout time.Time
	HeartbeatPeriod time.Duration
	StateMachine    StateMachine
	RaftTransport   RaftTransport
	DataTransport   Datatransport
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

func (node *RaftNode) Start() {

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
