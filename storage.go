package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

type StorageStateMachine struct {
	Storer Store
}

func (s *StorageStateMachine) Apply(cmd Command) ApplyResult {
	switch cmd.Type {
	case CommandWrite:
	//todo:
	case CommandDelete:
		//todo:
	}
	return ApplyResult{}
}

type StoreOpts struct {
	blockSize int
	root      string
}

// concurrent safe struct to read/write bytes to a CAS file system
type Store struct {
	StoreOpts
	mu    sync.RWMutex // mutex for the locks map
	locks map[string]*lock
}

type lock struct {
	sync.RWMutex     // per-file lock
	refCount     int // num of active operations
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
		locks:     make(map[string]*lock),
	}
}

// gets or creates a lock for given hash (key)
func (s *Store) getLock(key string) *lock {
	log.Printf("getting lock for %s", key)
	s.mu.Lock()
	defer s.mu.Unlock()
	l, exists := s.locks[key]
	if !exists {
		l = &lock{refCount: 0}
		s.locks[key] = l
	}
	l.refCount++
	return l
}

// releaseLock decrements the reference count and removes the lock if no longer needed
func (s *Store) releaseLock(key string) error {
	log.Printf("releasing lock for %s", key)
	s.mu.Lock()
	defer s.mu.Unlock()

	l, exists := s.locks[key]
	if !exists {
		return fmt.Errorf("cannot relase lock that does not exist for %s", key)
	}
	if l.refCount <= 0 {
		return fmt.Errorf("existing lock has LT 0 ref count")
	}
	l.refCount--
	if l.refCount == 0 {
		delete(s.locks, key)
	}
	return nil
}

// CreateAddress hashes a key (filename) into its expected path.
// FileAddress may not exist on file system.
// Always root + "/" + created_path
func (s *Store) CreateAddress(r io.Reader) (FileAddress, error) {
	hash := sha1.New()
	_, err := io.Copy(hash, r)
	if err != nil {
		return FileAddress{}, err
	}
	hashStr := hex.EncodeToString(hash.Sum(nil))
	return s.GetAddress(hashStr)
}

// GetAddress separates a HashStr into a FileAddress based on blocksize (max size of 1 directory name)
// and root
func (s *Store) GetAddress(hashStr string) (FileAddress, error) {
	directoryDepth := len(hashStr) / s.blockSize
	paths := make([]string, directoryDepth)
	for i := 0; i < directoryDepth; i++ {
		from, to := i*s.blockSize, (i*s.blockSize)+s.blockSize
		paths[i] = hashStr[from:to]
	}
	nRead := directoryDepth * s.blockSize
	leftOver := len(hashStr) - nRead

	// TODO: Fix hacky approach currently last directory may be up to 2*blockSize-1 long
	if leftOver != 0 {
		paths[len(paths)-1] = paths[len(paths)-1] + hashStr[nRead:nRead+leftOver]
	}

	joinedPaths := strings.Join(paths, "/")
	pathStr := filepath.Join(s.root, joinedPaths)
	return FileAddress{
		PathName: pathStr,
		HashStr:  hashStr,
	}, nil

}

// Delete removes the file and any empty sub-directories given a hash
func (s *Store) Delete(key string) error {
	fileLock := s.getLock(key)
	defer s.releaseLock(key)
	fileLock.Lock()
	defer fileLock.Unlock()

	path, err := s.GetAddress(key)
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
	address, err := s.GetAddress(key)
	if err != nil {
		return false
	}
	_, err = os.Stat(address.FullPath())
	return err == nil

}

// Read returs and io.Reader with the underlying bytes from the given key.
func (s *Store) Read(key string) (io.Reader, error) {
	fileLock := s.getLock(key)
	defer s.releaseLock(key)

	fileLock.RLock()
	defer fileLock.RUnlock()

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
	address, err := s.GetAddress(key)
	if err != nil {
		return nil, err
	}
	return os.Open(address.FullPath())
}

func (s *Store) Write(r io.Reader) (string, error) {
	buff := new(bytes.Buffer)
	tee := io.TeeReader(r, buff) // writes to buffer what it reads from R
	addr, err := s.CreateAddress(tee)
	if err != nil {
		return "", err
	}
	log.Println(buff.Bytes())

	fileLock := s.getLock(addr.HashStr)
	defer s.releaseLock(addr.HashStr)
	fileLock.Lock()
	defer fileLock.Unlock()

	return s.writeStream(buff)
}

// writeStream writes a file into our CAS.
func (s *Store) writeStream(r io.Reader) (string, error) {
	var buf1, buf2 bytes.Buffer
	r = io.TeeReader(r, io.MultiWriter(&buf1, &buf2)) // using teeReader to 'clone' the file to read twice (once for hash, another to save it)
	_, err := io.Copy(&buf1, r)
	if err != nil {
		return "", err
	}

	address, err := s.CreateAddress(&buf1)
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

type FileAddress struct {
	PathName string
	HashStr  string
}

// FullPath returns the full path to the file including the root directory
func (f *FileAddress) FullPath() string {
	return f.PathName + "/" + f.HashStr
}
