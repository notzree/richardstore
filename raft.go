package main

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
	AppendEntries()
}
