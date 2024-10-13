package p2p

import (
	"encoding/gob"
	"io"
)

// TODO: Im' 90% sure this Decoder message should read from the reader until it forms a complete packet and then
// Read it into the RPC struct.
type Decoder interface {
	Decode(io.Reader, *RPC) error // instead of []bytes you can just straight decode form a reader. Saves 1 decode from reader -> bytes -> Decoder
}

type GOBDecoder struct {
}

func (d *GOBDecoder) Decode(reader io.Reader, v *RPC) error {
	return gob.NewDecoder(reader).Decode(v)
}

type DefaultDecoder struct {
}

// TODO: Need to add framing to this decoding so I can actully read the data stream into a single cmd.
// Otherwise might have partial command data across multiple buffers
func (d *DefaultDecoder) Decode(reader io.Reader, v *RPC) error {
	buf := make([]byte, 64)
	n, err := reader.Read(buf)
	if err != nil {
		return err
	}
	v.Payload = buf[:n]
	return nil
}

type Encoder interface {
	Encode()
}
