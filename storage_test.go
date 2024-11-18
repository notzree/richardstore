package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateAddressFunc(t *testing.T) {
	content := "momsbestpicture"
	buf := bytes.NewBuffer([]byte(content))
	hash := sha1.New()
	io.Copy(hash, buf)
	hashStr := hex.EncodeToString(hash.Sum(nil))
	const BLOCKSIZE = 5
	fileAddress, err := CASCreateAddress(bytes.NewReader([]byte(content)), BLOCKSIZE, "root")
	if err != nil {
		panic(err)
	}
	assert.Equal(t, fileAddress.HashStr, hashStr)
	split := strings.Split(fileAddress.PathName, "/")
	index := 0
	for _, dir := range split[1:] { // skip the root directory b/c its always root + / + address
		assert.Equal(t, len(dir), BLOCKSIZE)
		expectedString := hashStr[index : index+BLOCKSIZE]
		assert.Equal(t, expectedString, dir)
		index += BLOCKSIZE
	}
}

func TestStore(t *testing.T) {
	opts := StoreOpts{
		CreateAddress: CASCreateAddress,
		GetAddress:    CASGetAddress,
		blockSize:     5,
		root:          "root",
	}
	s := NewStore(opts)
	data := []byte("cringe nft12222")
	buf := bytes.NewBuffer(data)
	// test write
	hash, err := s.writeStream(buf)
	if err != nil {
		t.Error(err)
	}
	// test read
	r, err := s.readStream(hash)
	if err != nil {
		t.Error(err)
	}
	readData, err := io.ReadAll(r)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, readData, data)
	// test delete
	err = s.Delete(hash)
	if err != nil {
		t.Error(err)
	}

}
