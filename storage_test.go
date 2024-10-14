package main

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateAddressFunc(t *testing.T) {
	content := strings.NewReader("momsbestpicture")
	hash := sha1.New()
	_, err := io.Copy(hash, content)
	if err != nil {
		panic(err)
	}
	const BLOCKSIZE = 5
	fileAddress, err := CASGetAddress(hash, BLOCKSIZE)
	if err != nil {
		panic(err)
	}
	split := strings.Split(fileAddress.PathName, "/")
	for _, dir := range split {
		assert.Equal(t, len(dir), BLOCKSIZE)
	}
}

func TestStore(t *testing.T) {
	opts := StoreOpts{
		CreateAddress: CASCreateAddress,
		GetAddress:    CASGetAddress,
		blockSize:     5,
	}
	s := NewStore(opts)

	data := bytes.NewBuffer([]byte("cringe nft12222"))
	hash, err := s.writeStream(data)
	if err != nil {
		t.Error(err)
	}
	r, err := s.readStream(hash)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("Read data:")
	io.Copy(os.Stdout, r)

}
