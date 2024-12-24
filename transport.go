package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

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

type TCPNode struct {
	conn     net.Conn
	outbound bool
}
type TCPData struct {
	r  io.Reader
	wg *sync.WaitGroup
}

func NewTCPNode(conn net.Conn, outbound bool) *TCPNode {
	return &TCPNode{
		conn:     conn,
		outbound: outbound,
	}
}

// gonna make this just pure tcp for now hardcoded wtv
type DataTransport struct {
	ln     net.Listener
	addr   net.Addr
	dataCh chan *TCPData
}

func NewDataTransport(addr net.Addr) *DataTransport {
	return &DataTransport{
		addr:   addr,
		dataCh: make(chan *TCPData),
	}
}
func (t *DataTransport) Consume() <-chan *TCPData {
	return t.dataCh
}

func (t *DataTransport) Start() error {
	var err error
	t.ln, err = net.Listen("tcp", t.addr.String())
	if err != nil {
		return err
	}
	log.Printf("data transport listening on port: %s\n", t.ln.Addr())
	t.startAcceptLoop()
	return nil
}

func (t *DataTransport) handShake(node *TCPNode) error {
	// noop for now, TODO: implement this
	return nil
}

// TODO
// then also implement ways to send bytes over the wire
// boom thats literally it
// Want to try to avoid TCPPeer bullshit but prob need to do it.
// Also want to look into the Primagen's way he mentioned on stream abt writing my own custom dec/enc, but that is out of scope
// for the transport. (maybe embed?)
// This NEEDs to stream the data. otherwise it will be horrendous for performance.
func (t *DataTransport) startAcceptLoop() {
	for {
		conn, err := t.ln.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			log.Printf("tcp connection err: %s\n", err)
			continue
		}
		peerNode := NewTCPNode(conn, false)
		go func() {
			t.handleConn(peerNode)
		}()
	}
}

func (t *DataTransport) handleConn(node *TCPNode) {
	var err error
	defer func() {
		fmt.Printf("closed peer connection : %v\n", err)
		node.conn.Close()
	}()
	if err = t.handShake(node); err != nil {
		return
	}
	tc := &TCPData{
		r:  node.conn,
		wg: &sync.WaitGroup{},
	}
	tc.wg.Add(1)
	t.dataCh <- tc
	tc.wg.Wait()
	fmt.Println("stream finished")
}
