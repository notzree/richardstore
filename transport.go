package main

import (
	"context"
	"log"
	"net"

	"github.com/notzree/richardstore/proto"
	"google.golang.org/grpc"
)

// type Transport[T any] interface {
// 	Consume() <-chan T // returns channel with specific type for processing
// 	Publish(ctx context.Context, msg T) error
// 	Dial(ctx context.Context, addr net.Addr) error
// 	ListenAndAccept() error
// 	Stop() error
// }

// Layer to make requests to other nodes over gRPC
type RaftTransport struct {
}
type DataTransport struct{}
type RaftServer struct {
	node *RaftNode
	proto.UnimplementedRaftServer
}

func RunRaftServer(node *RaftNode, listenAddr net.Addr) error {
	rs := RaftServer{node: node}
	ln, err := net.Listen("tcp", listenAddr.String())
	if err != nil {
		return err
	}
	opts := []grpc.ServerOption{}
	server := grpc.NewServer(opts...)
	proto.RegisterRaftServer(server, &rs)
	log.Printf("starting raft service on port %s\n", listenAddr.String())
	return server.Serve(ln)
}

// TODO: Figure out how the fuck this ties in with the Raftnode.
// I'm confused as to which should be the parent/ child yk.
// I think that the RaftNode should still own the RaftServer but if I do that there is a ciruclar dependency
// so legit IDK
// lowkey this might be fine.
// like we will just have to call RunRaftServer in the start() method of the raft node
// legit dont even need RaftServer, its just there to make the gRPC be nice
func (rs *RaftServer) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	return rs.node.HandleAppendEntriesRequest(ctx, req)
}

// func (rs *RaftServer) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {

// }
