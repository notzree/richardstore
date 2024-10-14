package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
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
	FileHash hash.Hash
}

func (f *FileAddress) FullPath() string {
	return f.PathName + f.FileName
}

// CreateAddress creates a Fileaddress given the contents of a file
type CreateAddress func(r io.Reader, blockSize int) (FileAddress, error)
type GetAddress func(key hash.Hash, blockSize int) (FileAddress, error)

// CASPathTransform hashes a key (filename) into its expected path.
// FileAddress may not exist on file system.
func CASCreateAddress(r io.Reader, blockSize int) (FileAddress, error) {
	hash := sha1.New()
	_, err := io.Copy(hash, r)
	if err != nil {
		return FileAddress{}, err
	}
	return CASGetAddress(hash, blockSize)
}

func CASGetAddress(key hash.Hash, blockSize int) (FileAddress, error) {
	hashStr := hex.EncodeToString(key.Sum(nil))
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
		FileHash: key,
	}, nil

}

type StoreOpts struct {
	CreateAddress CreateAddress
	GetAddress    GetAddress
	blockSize     int
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
	address, err := s.GetAddress(key, s.blockSize)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(address.FullPath())
	if err != nil {
		return nil, fmt.Errorf("could not find key %s", key)
	}
	return f, nil
}

// writeStream writes a file into our CAS.
func (s *Store) writeStream(r io.Reader) (hash.Hash, error) {
	var buf1, buf2 bytes.Buffer
	r = io.TeeReader(r, io.MultiWriter(&buf1, &buf2))
	_, err := io.Copy(&buf1, r)
	if err != nil {
		return nil, err
	}

	address, err := s.CreateAddress(&buf1, s.blockSize)
	if err != nil {
		return nil, err
	}

	fmt.Println(address.FullPath())

	// Create necessary directories
	if err := os.MkdirAll(address.PathName, fs.ModePerm); err != nil {
		return nil, err
	}

	// Create the file and copy the stream to it
	f, err := os.Create(address.FullPath())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Now copy the teeReader to the file, which will also update the hash
	n, err := io.Copy(f, &buf2)
	if err != nil {
		return nil, err
	}

	fmt.Printf("wrote %d bytes to disk, %s\n", n, address.FileName)
	return address.FileHash, nil
}
