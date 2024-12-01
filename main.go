package main

import (
	"log"

	"github.com/notzree/richardstore/p2p"
)

func makeServer(listenAddr string, nodes ...string) *FileServer {

	fileServerOpts2 := FileServerOpts{
		StoreOpts: StoreOpts{
			root:          listenAddr + "_network",
			CreateAddress: CASCreateAddress,
			GetAddress:    CASGetAddress,
		},
		TransportOpts: p2p.TCPTransportOpts{
			ListenAddr:    listenAddr,
			HandShakeFunc: p2p.NoopHandShakeFunc,
			Decoder:       &p2p.GOBDecoder{},
			// HandleNewNode: ,
			//TODO: add HandleNewNode func
		},
		BootstrapNodes: nodes,
	}
	return NewFileServer(fileServerOpts2)
}

func main() {
	fs1 := makeServer(":4000")
	go func() {
		log.Fatal(fs1.Start())
	}()
	fs2 := makeServer(":3000", ":4000")
	log.Fatal(fs2.Start())
	defer func() {
		fs1.quitch <- struct{}{}
		fs2.quitch <- struct{}{}
	}()
}
