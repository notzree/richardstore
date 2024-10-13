package p2p

import (
	"fmt"
	"io"
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

// Consume implements the Transport interface, returning a read only RPC channel.
// Used for reading incoming RPC messages sent from other nodes in network
func (t *TCPTransport) Consume() <-chan RPC {
	return t.rpcChan

}

func (t *TCPTransport) ListenAndAccept() error {
	var err error
	t.listener, err = net.Listen("tcp", t.TCPTransportOpts.ListenAddr)
	if err != nil {
		return err
	}
	t.startAcceptLoop()
	return nil

}

func (t *TCPTransport) startAcceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			fmt.Printf("tcp connection err: %s\n", err)
			continue
		}
		go t.startReadLoop(conn)
	}
}

// startReadLoop will read data from tcp connection
func (t *TCPTransport) startReadLoop(conn net.Conn) {
	var err error
	defer func() {
		fmt.Printf("closed peer connection: %v\n", err)
		conn.Close()
	}()
	peerNode := NewTCPNode(conn, true)
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
