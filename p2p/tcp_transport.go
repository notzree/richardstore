package p2p

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
)

// TCPPeer represents a node in the TCP network
type TCPNode struct {
	conn net.Conn
	// outbound if the node is making the connection to the server
	outbound bool
}

func NewTCPNode(conn net.Conn, outbound bool) *TCPNode {
	return &TCPNode{
		conn:     conn,
		outbound: outbound,
	}
}

func (n *TCPNode) Close() error {
	return n.conn.Close()
}

type TCPTransportOpts struct {
	ListenAddr    string
	HandShakeFunc HandShakeFunc
	Decoder       Decoder
	HandleNewNode HandleNewNode
}

// TCPTransport represents the TCP protocol
type TCPTransport struct {
	TCPTransportOpts TCPTransportOpts
	listener         net.Listener
	rpcChan          chan RPC
}

func NewTCPTransport(opts TCPTransportOpts) *TCPTransport {
	return &TCPTransport{
		TCPTransportOpts: opts,
		rpcChan:          make(chan RPC),
	}
}

// Dial will dial an addr and start reading data from it.
func (t *TCPTransport) Dial(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	go t.handleConn(conn, true)
	return nil
}

// Consume implements the Transport interface, returning a read only RPC channel.
// Used for reading incoming RPC messages sent from other nodes in network
func (t *TCPTransport) Consume() <-chan RPC {
	return t.rpcChan
}

// Close implements Transport interface
func (t *TCPTransport) Close() error {
	return t.listener.Close()
}

func (t *TCPTransport) ListenAndAccept() error {
	var err error
	t.listener, err = net.Listen("tcp", t.TCPTransportOpts.ListenAddr)
	if err != nil {
		return err
	}
	go t.startAcceptLoop()
	log.Printf("TCP Transport listening on port: %s\n", t.listener.Addr())
	return nil

}

func (t *TCPTransport) startAcceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			fmt.Printf("tcp connection err: %s\n", err)
			continue
		}
		// Accepting == inbound
		go t.handleConn(conn, false)
	}
}

// handleConn will read data from tcp connection
func (t *TCPTransport) handleConn(conn net.Conn, outbound bool) {
	var err error
	defer func() {
		fmt.Printf("closed peer connection: %v\n", err)
		conn.Close()
	}()
	peerNode := NewTCPNode(conn, outbound)
	// MUST shake hands before reading
	if err = t.TCPTransportOpts.HandShakeFunc(peerNode); err != nil {
		return
	}
	// IF provided HandleNode function errors, we close the connection
	if t.TCPTransportOpts.HandleNewNode != nil {
		if err = t.TCPTransportOpts.HandleNewNode(peerNode); err != nil {
			return
		}
	}
	log.Printf("local: %s | remote: %s", conn.LocalAddr(), conn.RemoteAddr())
	rpc := RPC{}
	for {
		err := t.TCPTransportOpts.Decoder.Decode(peerNode.conn, &rpc)
		if err != nil {
			if err == io.EOF {
				break
			}
			if opErr, ok := err.(*net.OpError); ok {
				err = opErr
				fmt.Printf("%v", err)
				return
			}
			fmt.Printf("err reading from connection: %T\n", err)
		}
		rpc.From = conn.RemoteAddr()
		t.rpcChan <- rpc
	}

}
