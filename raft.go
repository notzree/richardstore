package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/notzree/richardstore/proto"
	pr "google.golang.org/protobuf/proto"
)

var MAX_APPEND_ENTRIES_BATCH = 5

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

	RaftTransport RaftTransport

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
	logs []*proto.LogEntry

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
	raftTransport *RaftTransport

	// dataManager exposes the storer and data transport to the raft node
	dataManager *DataManager

	logStorer      SnapshotStorer[[]*proto.LogEntry] // TODO: In the future maybe need to make this more efficient
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
	RaftTransport   *RaftTransport
	DataManager     *DataManager
	SnapshotStorer  SnapshotStorer[PersistedRaftFields]
	Cluster         []PeerRaftNode
	logs            []*proto.LogEntry
}

func NewRaftNode(cfg RaftNodeConfig) *RaftNode {
	var peers []PeerRaftNode
	for _, c := range cfg.Cluster {
		if c.Id == 0 {
			panic("peer id not allowed to be zero")
		}
		c.RaftTransport = *NewRaftTansport(c.RPCAddress)
		if err := c.RaftTransport.Dial(); err != nil {
			panic(fmt.Errorf("error initializing gRPC connection with peer %v", c.Id))
		}
		peers = append(peers, c)
	}
	logs := cfg.logs
	if logs == nil {
		logs = make([]*proto.LogEntry, 0)
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
		dataManager:      cfg.DataManager,
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
	go func() {
		log.Fatal(node.dataManager.Listen())
	}()
}

func (node *RaftNode) Apply() {

}

func (node *RaftNode) appendEntries() {
	if node.state != Leader {
		return
	}
	for i, peerNode := range node.peers {
		go func(i int, peerNode *PeerRaftNode) {
			node.mu.Lock()
			next := node.peers[i].nextIndex
			prevLogIndex := next - 1
			prevLogTerm := node.logs[prevLogIndex].Term
			var entries []*proto.LogEntry
			// check if we actually have things to send.
			// if the index of our logs is not greater than or eq to nextIndex, peer might alr be caught up
			if uint64(len(node.logs)-1) >= node.peers[i].nextIndex {
				log.Printf("len: %d, next: %d, server: %d", len(node.logs), next, node.peers[i].Id)
				entries = node.logs[next:]
			}
			// trim to max_entries_length
			if len(entries) > MAX_APPEND_ENTRIES_BATCH {
				entries = entries[:MAX_APPEND_ENTRIES_BATCH]
			}
			lenEntries := uint64(len(entries))
			req := &proto.AppendEntriesRequest{
				Term:         node.CurrentTerm,
				LeaderId:     node.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: node.commitIndex,
			}
			node.mu.Unlock()
			ctx := context.Background()
			log.Printf("Sending %d entries to %d for term %d.", lenEntries, node.peers[i].Id, req.Term)
			resp, err := peerNode.RaftTransport.appendEntries(ctx, req)
			if err != nil {
				// retry next tick
				return
			}
			node.mu.Lock()
			defer node.mu.Unlock()
			if node.updateTerm(resp.Term) {
				return // no longer leader
			}
			if dropStaleResponse := resp.Term != req.Term && node.state == Leader; dropStaleResponse {
				return
			}
			if resp.Success {
				prev := node.peers[i].nextIndex // previous next index
				// set the new nextIndex to the index of the request + length of new entries (all of the logs read) + 1
				node.peers[i].nextIndex = max(prev+lenEntries, 1) // more readable
				node.peers[i].matchIndex = node.peers[i].nextIndex - 1
				log.Printf("message accepted for %d. Prev Index: %d, Next Index: %d, Match Index: %d.", node.peers[i].Id, prev, node.peers[i].nextIndex, node.peers[i].matchIndex)
			} else {
				node.peers[i].nextIndex = max(node.peers[i].nextIndex-1, 1)
				log.Printf("message rejected, rolled back logs to %d for %d", node.peers[i].nextIndex, node.peers[i].Id)
			}
		}(i, &peerNode)
	}
}

func (node *RaftNode) HandleAppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {

	// term < currentTerm or prev log index is out of bounds
	if req.Term < node.CurrentTerm || req.PrevLogIndex > uint64(max(0, len(node.logs)-1)) {
		return node.respAppendEntries(false), nil
	}
	node.mu.Lock()
	defer node.mu.Unlock()
	node.updateTerm(req.Term)
	node.resetElectionTimer()

	// log does not contain an entry at prevLogIndex whose term matches prevLogTerm
	prevLog := node.logs[req.PrevLogIndex]
	if prevLog.Term != req.PrevLogTerm {
		return node.respAppendEntries(false), nil
	}

	// Check to ensure all required data is in local fs
	for _, entry := range req.Entries {
		if entry.Cmd.Type == proto.CommandType_COMMAND_TYPE_WRITE {
			var cmd proto.WriteCommand
			if err := DeserializeCommand(entry.Cmd, &cmd); err != nil {
				return nil, fmt.Errorf("failed to deserialize write cmd")
			}
			// Hash is not in system, respond with false
			if !node.dataManager.Has(string(cmd.Hash)) {
				return node.respAppendEntries(false), nil
			}
		}
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	newEntries := make([]*proto.LogEntry, 0) // Changed to non-pointer slice
	for i, entry := range req.Entries {
		if entry.Index < uint64(len(node.logs)) {
			if node.logs[entry.Index].Term != entry.Term {
				// conflict found, truncate logs from this point
				node.logs = node.logs[:entry.Index]
				// Convert pointer entries to values
				for _, e := range req.Entries[i:] {
					newEntries = append(newEntries, &proto.LogEntry{
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
				newEntries = append(newEntries, &proto.LogEntry{
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

	if req.LeaderCommit > node.commitIndex {
		node.commitIndex = min(req.LeaderCommit, uint64(max(0, len(node.logs)-1)))
		go func() {
			if err := node.applyCommittedEntries(); err != nil {
				log.Printf("error applying committed entries up to commit index %v: %v", node.commitIndex, err)
			}
		}()
	}

	return node.respAppendEntries(true), nil
}

func (node *RaftNode) advanceCommitIndex() {
	node.mu.Lock()
	defer node.mu.Unlock()

}

// Must be called within mutex lock
func (node *RaftNode) updateTerm(incomingTerm uint64) bool {
	transitioned := false
	if incomingTerm > node.CurrentTerm {
		node.CurrentTerm = incomingTerm
		node.state = Follower
		node.setVotedFor(0)
		transitioned = true
		log.Print("transitioned to follower\n")
		node.resetElectionTimer()
		node.persist(false)
	}
	return transitioned
}

// Must be called within mutex lock
func (node *RaftNode) setVotedFor(id uint64) {
	node.VotedFor = id
}

// Must be called within mutex lock
func (node *RaftNode) applyCommittedEntries() error {
	for node.commitIndex > node.lastApplied {
		node.lastApplied++
		log := node.logs[node.lastApplied]
		_, err := node.statemachine.Apply(log.Cmd)
		if err != nil {
			return err
		}
	}
	return nil
}

func (node *RaftNode) resetElectionTimer() {
	//TODO: Implement this

}

func (node *RaftNode) respAppendEntries(success bool) *proto.AppendEntriesResponse {
	return &proto.AppendEntriesResponse{
		Term:    node.CurrentTerm,
		Success: success,
	}
}

// Persists currentTerm + log to metadata storage
func (node *RaftNode) persist(writeLogs bool) error {
	if writeLogs {
		if err := node.logStorer.Persist(node.logs); err != nil {
			return err
		}
	}
	if err := node.metadataStorer.Persist(node.PersistedRaftFields); err != nil {
		return err
	}
	return nil
}

//TODO:

// What if instead, each server exposes a fixed datatransfer port.

// Then, when the leader decides to append a write command, it will go ahead and send that file over to the other servers via a TCP connection on the data transfer port. Since I'm using a CAS file system, the incoming files can be read and stored. Then in the writecmd, the hash for each file can be checked. The nodes will maintain an internal list of files that have not been confirmed (This could be reasons like Consensus was not reached, but transfer still happened). Then, after each term, the nodes will periodically clear our the orphaned files.

// This would sort of mean that the data transfer is taking place asynchronously from the log propogation.
