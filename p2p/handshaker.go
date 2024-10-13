package p2p

import "errors"

// Returned if handshake between 2 nodes could not be established.
var ErrInvalidHandshake = errors.New("invalid handshake")

type HandShakeFunc func(Node) error

func NoopHandShakeFunc(Node) error {
	return nil
}
