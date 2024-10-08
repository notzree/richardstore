package p2p

// Node represents a node in the network
type Node interface {
}

// Transport is any protocol that handles communication between 2 nodes in network
type Transport interface {
	ListenAndAccept() error
}
