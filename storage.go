package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"strings"
)

type FileAddress struct {
	PathName string
	FileName string
}

func (f *FileAddress) FullPath() string {
	return f.PathName + f.FileName
}

// LocateFile turns a hash into a FileAdress
type LocateFile func(hash hash.Hash, blockSize int) FileAddress

// CASPathTransform hashes a key (filename) into a path.
// FileAddress may not exist on file system.
func CASLocate(key hash.Hash, blockSize int) FileAddress {
	hashStr := hex.EncodeToString(key.Sum(nil))
	fmt.Println(len(hashStr))
	directoryDepth := len(hashStr) / blockSize
	paths := make([]string, directoryDepth)

	for i := 0; i < directoryDepth; i++ {
		from, to := i*blockSize, (i*blockSize)+blockSize
		paths[i] = hashStr[from:to]
	}
	nRead := directoryDepth * blockSize
	leftOver := len(hashStr) - nRead

	// TODO: Fix hacky approach, last directory may be up to 2*blockSize-1 long
	if leftOver != 0 {
		paths[len(paths)-1] = paths[len(paths)-1] + hashStr[nRead:nRead+leftOver]
	}

	pathStr := strings.Join(paths, "/")

	return FileAddress{
		PathName: pathStr,
		FileName: hashStr,
	}
}

type StoreOpts struct {
	LocateFile LocateFile
	blockSize  int
}

type Store struct {
	StoreOpts
}

func NewStore(opts StoreOpts) *Store {
	return &Store{
		StoreOpts: opts,
	}
}

// readStream reads a file from a Hash
func (s *Store) readStream(key hash.Hash) (io.Reader, error) {
	address := s.LocateFile(key, s.blockSize)
	f, err := os.Open(address.FullPath())
	if err != nil {
		return nil, errors.New(fmt.Sprint("could not find key %s", key))
	}
	return f, nil
}

// writeStream writes a file into our CAS.
func (s *Store) writeStream(r io.Reader) error {
	hash := sha1.New()
	rCopy := new(bytes.Buffer)
	mw := io.MultiWriter(hash, rCopy)
	_, err := io.Copy(mw, r)
	if err != nil {
		return err
	}
	// Use io.TeeReader to write the data to the hash and pass it through
	// Create the address after hashing and before writing
	address := s.LocateFile(hash, s.blockSize)
	fmt.Println(address.FullPath())

	// Create necessary directories
	if err := os.MkdirAll(address.PathName, fs.ModePerm); err != nil {
		return err
	}

	// Create the file and copy the stream to it
	f, err := os.Create(address.FullPath())
	if err != nil {
		return err
	}
	defer f.Close()

	// Now copy the teeReader to the file, which will also update the hash
	n, err := io.Copy(f, rCopy)
	if err != nil {
		return err
	}

	fmt.Printf("wrote %d bytes to disk, %s\n", n, address.FileName)
	return nil
}
