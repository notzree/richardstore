package p2p

import (
	"fmt"
	"io"
	"net"
	"sync"
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

// TCPTransport represents the TCP protocol
type TCPTransport struct {
	listenAddr string
	listener   net.Listener
	shakeHands HandShakeFunc

	numConLock               sync.Mutex
	numConnections           int
	maxConcurrentConnections int

	peersLock sync.RWMutex
	peers     map[net.Addr]Node
}

func NewTCPTransport(listenAddr string) *TCPTransport {
	return &TCPTransport{
		listenAddr: listenAddr,
		shakeHands: func(any) error {
			return nil
		},
		numConnections:           0,
		maxConcurrentConnections: 0,
	}
}

func (t *TCPTransport) ListenAndAccept() error {
	var err error
	t.listener, err = net.Listen("tcp", t.listenAddr)
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
	fmt.Printf("new incoming connection %+v\n", conn)
	go func() {
		t.numConLock.Lock()
		defer t.numConLock.Unlock()
		t.numConnections += 1
		t.maxConcurrentConnections = max(t.maxConcurrentConnections, t.numConnections)

	}()
	peerNode := NewTCPNode(conn, true)
	buffer := make([]byte, 4096)
	for {
		n, err := peerNode.conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				fmt.Printf("connection ended: %+v\n", conn)
				go func() {
					t.numConLock.Lock()
					defer t.numConLock.Unlock()
					t.numConnections -= 1
				}()
				break
			}
			fmt.Printf("err reading from connection: %s\n", err)
		}
		fmt.Println(string(buffer[:n]))
	}

}
