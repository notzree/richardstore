package main

import (
	"log"

	"github.com/notzree/richardstore/p2p"
)

func main() {
	listenAddr := ":4000"
	tcp := p2p.NewTCPTransport(listenAddr)
	log.Fatal(tcp.ListenAndAccept())

}
