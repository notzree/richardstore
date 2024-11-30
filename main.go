package main

import (
	"log"
	"time"

	"github.com/notzree/richardstore/p2p"
)

func MockHandleNode(node p2p.Node) error {
	// fmt.Printf("new incoming connection %v\n", node)
	node.Close()
	return nil
}

func main() {
	// go func() {
	// 	listenAddr2 := ":3000"
	// 	fileServerOpts2 := FileServerOpts{
	// 		StoreOpts: StoreOpts{
	// 			root:          listenAddr2 + "_network",
	// 			CreateAddress: CASCreateAddress,
	// 			GetAddress:    CASGetAddress,
	// 		},
	// 		TransportOpts: p2p.TCPTransportOpts{
	// 			ListenAddr:    listenAddr2,
	// 			HandShakeFunc: p2p.NoopHandShakeFunc,
	// 			Decoder:       &p2p.GOBDecoder{},
	// 			// HandleNewNode: ,
	// 			//TODO: add HandleNewNode func
	// 		},
	// 	}
	// 	fs2 := NewFileServer(fileServerOpts2)
	// 	go func() {
	// 		time.Sleep(time.Second * 3)
	// 		fs2.Stop()
	// 	}()
	// 	if err := fs2.Start(); err != nil {
	// 		log.Fatal(err)
	// 	}

	// }()
	listenAddr := ":4000"

	fileServerOpts := FileServerOpts{
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
		BootstrapNodes: []string{":3000"},
	}

	fs := NewFileServer(fileServerOpts)
	go func() {
		time.Sleep(time.Second * 3)
		fs.Stop()
	}()

	if err := fs.Start(); err != nil {
		log.Fatal(err)
	}
}
