package p2p

// Node represents a node in the network
type Node interface {
	Close() error
}

// HandleNewNode is a function that can be passed onto a transport to respond to a new node being connected
// over the transport. Can be used by a server to maintain a map of connections, etc
type HandleNewNode func(Node) error

// Transport is any protocol that handles communication between 2 nodes in network
type Transport interface {
	ListenAndAccept() error
	Consume() <-chan RPC // read only channel
}
