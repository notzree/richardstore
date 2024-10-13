package p2p

import "net"

// RPC holds any data that is being sent over each transport between 2 nodes in the network
type RPC struct {
	Payload []byte
	From    net.Addr
}
