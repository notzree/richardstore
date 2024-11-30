package main

import (
	"fmt"
	"log"

	"github.com/notzree/richardstore/p2p"
)

type FileServerOpts struct {
	StoreOpts      StoreOpts
	TransportOpts  p2p.TCPTransportOpts
	BootstrapNodes []string
}

type FileServer struct {
	BootstrapNodes []string
	store          *Store
	transport      p2p.Transport
	quitch         chan struct{}
}

func NewFileServer(opts FileServerOpts) *FileServer {
	return &FileServer{
		store:          NewStore(opts.StoreOpts),
		transport:      p2p.NewTCPTransport(opts.TransportOpts),
		quitch:         make(chan struct{}),
		BootstrapNodes: opts.BootstrapNodes,
	}
}

func (s *FileServer) bootstrapNetwork() error {
	for _, addr := range s.BootstrapNodes {
		log.Println("dialing: ", addr)
		go func(addr string) {
			if err := s.transport.Dial(addr); err != nil {
				log.Printf("error dialing %s: %v \n", addr, err)
			}
		}(addr)

	}
	return nil
}

func (s *FileServer) Stop() {
	close(s.quitch)
}

func (s *FileServer) Loop() {
	defer func() {
		log.Println("file server stopped due to user quit action")
		s.transport.Close()
	}()

	for {
		select {
		case msg := <-s.transport.Consume():
			fmt.Println(msg)
		case <-s.quitch:
			return
		}
	}
}

func (s *FileServer) Start() error {
	if err := s.transport.ListenAndAccept(); err != nil {
		return err
	}
	s.bootstrapNetwork()
	s.Loop()
	return nil
}
