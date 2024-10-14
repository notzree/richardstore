package main

import (
	"bytes"
	"testing"
)

// func TestPathTransformFunc(t *testing.T) {
// 	key := "momsbestpicture"
// 	const BLOCKSIZE = 5
// 	pathName := CASLocate(key, BLOCKSIZE)
// 	split := strings.Split(pathName, "/")
// 	for _, dir := range split {
// 		assert.Equal(t, len(dir), BLOCKSIZE)
// 	}
// }

func TestStore(t *testing.T) {
	fileName := "hurhurhur"
	opts := StoreOpts{
		LocateFile: CASLocate,
		blockSize:  5,
	}
	s := NewStore(opts)

	data := bytes.NewBuffer([]byte("cringe nft12222"))
	err := s.writeStream(fileName, data)
	if err != nil {
		t.Error(err)
	}

}
