package main

import (
	"context"
	"fmt"
	"log"
	"sync"

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
	peerLock       sync.Mutex
	peers          map[string]p2p.Node
}

func NewFileServer(opts FileServerOpts) *FileServer {
	return &FileServer{
		store:          NewStore(opts.StoreOpts),
		transport:      p2p.NewTCPTransport(opts.TransportOpts),
		quitch:         make(chan struct{}),
		BootstrapNodes: opts.BootstrapNodes,
		peerLock:       sync.Mutex{},
		peers:          make(map[string]p2p.Node),
	}
}

func (s *FileServer) bootstrapNetwork() error {
	for _, addr := range s.BootstrapNodes {
		if len(addr) == 0 {
			continue
		}
		log.Println("dialing: ", addr)
		go func(addr string) {
			ctx := context.Background()
			if err := s.transport.Dial(ctx, addr); err != nil {
				log.Printf("error dialing %s: %v \n", addr, err)
			}
		}(addr)

	}
	return nil
}

func (s *FileServer) Stop() {
	close(s.quitch)
}
func (s *FileServer) handleEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		//receive an event
		case event := <-s.transport.Events():
			switch event.EventType {
			case p2p.EventTypeNodeNew:
				err := s.HandleNewNode(event.RemoteNode)
				event.ResponseCh <- err
			}
			//TODO: add more event types as needed

		}
	}
}

func (s *FileServer) HandleNewNode(node p2p.Node) error {
	fmt.Println("handling new node")
	return nil
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
	rootContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.transport.ListenAndAccept(rootContext); err != nil {
		return err
	}
	go s.handleEvents(rootContext)
	s.bootstrapNetwork()
	s.Loop()
	return nil
}
