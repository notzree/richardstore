package main

type Server struct {
	Raft *RaftNode
}

func NewServer(cfg RaftNodeConfig) *Server {
	return &Server{
		Raft: NewRaftNode(cfg),
	}
}
