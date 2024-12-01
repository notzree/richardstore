package p2p

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

// TCPPeer represents a node in the TCP network and implements the Node interface
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
}

// TCPTransport implements the full Transport Protocol
type TCPTransport struct {
	TCPTransportOpts TCPTransportOpts
	listener         net.Listener
	rpcChan          chan RPC
	eventChan        chan Event
}

func NewTCPTransport(opts TCPTransportOpts) *TCPTransport {
	return &TCPTransport{
		TCPTransportOpts: opts,
		rpcChan:          make(chan RPC),
		eventChan:        make(chan Event),
	}
}

// Dial will dial an addr and start reading data from it.
func (t *TCPTransport) Dial(ctx context.Context, addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	go t.handleConn(ctx, conn, true)
	return nil
}

// Consume implements the Transport interface, returning a read only RPC channel.
// Used for reading incoming RPC messages sent from other nodes in network
func (t *TCPTransport) Consume() <-chan RPC {
	return t.rpcChan
}
func (t *TCPTransport) Events() <-chan Event {
	return t.eventChan
}

// Close implements Transport interface
func (t *TCPTransport) Close() error {
	return t.listener.Close()
}

func (t *TCPTransport) ListenAndAccept(ctx context.Context) error {
	var err error
	t.listener, err = net.Listen("tcp", t.TCPTransportOpts.ListenAddr)
	if err != nil {
		return err
	}
	go t.startAcceptLoop(ctx)
	log.Printf("TCP Transport listening on port: %s\n", t.listener.Addr())
	return nil

}

func (t *TCPTransport) startAcceptLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			t.Close()
		default:
			conn, err := t.listener.Accept()
			if errors.Is(err, net.ErrClosed) {
				return
			}
			if err != nil {
				fmt.Printf("tcp connection err: %s\n", err)
				continue
			}
			eventResponseCtx, cancel := context.WithTimeout(ctx, time.Second*5)
			go func() {
				defer cancel()
				// Accepting == inbound
				t.handleConn(eventResponseCtx, conn, false)
			}()
		}
	}
}

// handleConn will read data from tcp connection
func (t *TCPTransport) handleConn(ctx context.Context, conn net.Conn, outbound bool) {
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
	newNodeEvent := NewEvent(EventTypeNodeNew, peerNode)
	t.eventChan <- newNodeEvent
	select {
	case err := <-newNodeEvent.ResponseCh:
		if err != nil {
			conn.Close()
			return
		}
	case <-ctx.Done():
		//context timed out or cancelled
		conn.Close()
		return
	}

	log.Printf("node: %s received incoming connection: %s", conn.LocalAddr(), conn.RemoteAddr())
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
