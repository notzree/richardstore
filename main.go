package main

import (
	"fmt"
	"log"

	"github.com/notzree/richardstore/p2p"
)

func MockHandleNode(node p2p.Node) error {
	// fmt.Printf("new incoming connection %v\n", node)
	node.Close()
	return nil
}

func main() {
	listenAddr := ":4000"

	opts := p2p.TCPTransportOpts{
		ListenAddr:    listenAddr,
		HandShakeFunc: p2p.NoopHandShakeFunc,
		Decoder:       &p2p.DefaultDecoder{},
		HandleNewNode: MockHandleNode,
	}
	tcp := p2p.NewTCPTransport(opts)

	go func() {
		for {
			msg := <-tcp.Consume()
			fmt.Printf("msg: %+v\n", string(msg.Payload))
		}
	}()
	log.Fatal(tcp.ListenAndAccept())

}
