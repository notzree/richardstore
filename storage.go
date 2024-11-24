package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type FileAddress struct {
	PathName string
	HashStr  string
}

// FullPath returns the full path to the file including the root directory
func (f *FileAddress) FullPath() string {
	return f.PathName + "/" + f.HashStr
}

// CreateAddress creates an address from a reader
type CreateAddress func(r io.Reader, blockSize int, root string) (FileAddress, error)

// GetAddress gets the address from a hash
type GetAddress func(key string, blockSize int, root string) (FileAddress, error)

// CASPathTransform hashes a key (filename) into its expected path.
// FileAddress may not exist on file system.
// Always root + "/" + created_path
func CASCreateAddress(r io.Reader, blockSize int, root string) (FileAddress, error) {
	hash := sha1.New()
	_, err := io.Copy(hash, r)
	if err != nil {
		return FileAddress{}, err
	}
	hashStr := hex.EncodeToString(hash.Sum(nil))
	return CASGetAddress(hashStr, blockSize, root)
}

// CASGetAddress separates a HashStr into a FileAddress based on blocksize (max size of 1 directory name)
// and root
func CASGetAddress(hashStr string, blockSize int, root string) (FileAddress, error) {
	directoryDepth := len(hashStr) / blockSize
	paths := make([]string, directoryDepth)
	for i := 0; i < directoryDepth; i++ {
		from, to := i*blockSize, (i*blockSize)+blockSize
		paths[i] = hashStr[from:to]
	}
	nRead := directoryDepth * blockSize
	leftOver := len(hashStr) - nRead

	// TODO: Fix hacky approach currently last directory may be up to 2*blockSize-1 long
	if leftOver != 0 {
		paths[len(paths)-1] = paths[len(paths)-1] + hashStr[nRead:nRead+leftOver]
	}

	joinedPaths := strings.Join(paths, "/")
	pathStr := filepath.Join(root, joinedPaths)
	return FileAddress{
		PathName: pathStr,
		HashStr:  hashStr,
	}, nil

}

type StoreOpts struct {
	CreateAddress CreateAddress
	GetAddress    GetAddress
	blockSize     int
	root          string
}

type Store struct {
	StoreOpts
}

func NewStore(opts StoreOpts) *Store {
	if opts.root == "" {
		// If no root is specified, use the current working directory
		cwd, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		opts.root = cwd
	}
	return &Store{
		StoreOpts: opts,
	}
}

// Delete removes the file and any empty sub-directories given a hash
func (s *Store) Delete(key string) error {
	path, err := s.GetAddress(key, s.blockSize, s.root)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path.FullPath())
	fmt.Println("full path:", dir)
	// delete file
	err = os.Remove(path.FullPath())
	if err != nil {
		return err
	}
	// remove empty directories excluding root directory
	for dir != s.root {
		err = os.Remove(dir)
		if err != nil {
			if os.IsNotExist(err) {
				// Directory already deleted, continue to parent
				dir = filepath.Dir(dir)
				continue
			}
			if err, ok := err.(*os.PathError); ok && err.Err == syscall.ENOTEMPTY {
				// Directory not empty, return
				break
			}
			return err
		}
		dir = filepath.Dir(dir)
	}
	return nil
}

// Has checks if the Storage object has stored a key before
func (s *Store) Has(key string) bool {
	address, err := s.GetAddress(key, s.blockSize, s.root)
	if err != nil {
		return false
	}
	_, err = os.Stat(address.FullPath())
	return err == nil

}

func (s *Store) Read(key string) (io.Reader, error) {
	f, err := s.readStream(key)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, f)
	return buf, err
}

// readStream reads a file from a Hash
func (s *Store) readStream(key string) (io.ReadCloser, error) {
	address, err := s.GetAddress(key, s.blockSize, s.root)
	if err != nil {
		return nil, err
	}
	return os.Open(address.FullPath())
}

func (s *Store) Write(r io.Reader) (string, error) {
	return s.writeStream(r)
}

// writeStream writes a file into our CAS.
func (s *Store) writeStream(r io.Reader) (string, error) {
	var buf1, buf2 bytes.Buffer
	r = io.TeeReader(r, io.MultiWriter(&buf1, &buf2)) // using teeReader to 'clone' the file to read twice (once for hash, another to save it)
	_, err := io.Copy(&buf1, r)
	if err != nil {
		return "", err
	}

	address, err := s.CreateAddress(&buf1, s.blockSize, s.root)
	if err != nil {
		return "", err
	}

	// Create necessary directories
	if err := os.MkdirAll(address.PathName, fs.ModePerm); err != nil {
		return "", err
	}

	// Create the file and copy the stream to it
	f, err := os.Create(address.FullPath())
	if err != nil {
		return "", err
	}
	defer f.Close()

	n, err := io.Copy(f, &buf2)
	if err != nil {
		return "", err
	}

	fmt.Printf("wrote %d bytes to disk, %s\n", n, address.HashStr)
	return address.HashStr, nil
}

// Clear deletes the root and all subdirectories
func (s *Store) Clear() error {
	return os.RemoveAll(s.root)
}
