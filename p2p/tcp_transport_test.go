package p2p

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTCPTransport(t *testing.T) {
	listenAddr := ":4000"
	trOpts := TCPTransportOpts{
		ListenAddr:    ":4000",
		HandShakeFunc: NoopHandShakeFunc,
		Decoder:       &GOBDecoder{},
	}
	tr := NewTCPTransport(trOpts)
	go func() {
		assert.Nil(t, tr.ListenAndAccept())
	}()
	time.Sleep(100 * time.Millisecond)

	// Number of concurrent connections to test
	numConnections := 1000
	var wg sync.WaitGroup
	doneCh := make(chan bool)

	for i := 0; i < numConnections; i++ {
		wg.Add(1)
		go func(done chan bool) {
			conn, err := net.Dial("tcp", listenAddr)
			if err != nil {
				return
			}
			wg.Done() // tell wg that we are done creating connection.
			for {
				select {
				case <-done:
					conn.Close()
					return
				default:
					time.Sleep(50 * time.Millisecond)
				}
			}
		}(doneCh)
		time.Sleep(5 * time.Millisecond)
	}
	wg.Wait()
	doneCh <- true

}
