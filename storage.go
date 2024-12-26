package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
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

	"github.com/notzree/richardstore/proto"
)

type StorageStateMachine struct {
	Storer Store
}

func (s *StorageStateMachine) Apply(cmd *proto.Command) (*ApplyResult, error) {
	switch cmd.Type {
	case proto.CommandType_COMMAND_TYPE_UNSPECIFIED:
		// will return empty ApplyResult
	case proto.CommandType_COMMAND_TYPE_WRITE:
		var spec_cmd proto.WriteCommand
		if err := DeserializeCommand(cmd, &spec_cmd); err != nil {
			return nil, err
		}
		return &ApplyResult{}, nil

	case proto.CommandType_COMMAND_TYPE_DELETE:
		// todo
	case proto.CommandType_COMMAND_TYPE_CONFIG:
		// unimplemented
	}
	return &ApplyResult{}, nil
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

var dFILE_METADATA = "_metadata"

// dFileMetadata is metadata about files shared on a distributed network. Used to verify if a file has been completely read,
// and also which nodes to query for the file.
type dFileMetadata struct {
	Hash   string
	Size   uint64
	Owners []uint64 // other nodes that hold copies of this
}

// dFile is a file on a distributed system. it might contain a reader where one can actually read the data. If not, it contains
// dFileMetadata which contains data on what nodes to query for the file.
type dFile struct {
	Reader io.ReadCloser
	dFileMetadata
}

func NewdFile(r io.ReadCloser, metadata dFileMetadata) *dFile {
	return &dFile{
		Reader:        r,
		dFileMetadata: metadata,
	}
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

// Read returns a dFile which either will contain a reader, or a reference to the file on the network
func (s *Store) Read(key string) (*dFile, error) {
	fileLock := s.getLock(key)
	defer s.releaseLock(key)

	fileLock.RLock()
	defer fileLock.RUnlock()

	f, err := s.readStream(key)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// readStream reads a file from a Hash
func (s *Store) readStream(key string) (*dFile, error) {
	var df *dFile

	address, err := s.GetAddress(key)
	if err != nil {
		return nil, err
	}
	metadataFile, err := os.Open(address.MetadataPath())
	if err != nil {
		return nil, err
	}
	defer metadataFile.Close()
	dec := gob.NewDecoder(metadataFile)
	var metadata dFileMetadata
	if err := dec.Decode(&metadata); err != nil {
		return nil, err
	}
	df = &dFile{
		Reader:        nil,
		dFileMetadata: metadata,
	}
	if _, err := os.Stat(address.FullPath()); os.IsNotExist(err) {
		// file not on system but metadata is
	} else if err != nil {
		return nil, err
	}

	file, err := os.Open(address.FullPath())
	if err != nil {
		return nil, err
	}
	df.Reader = file
	return df, nil
}

// writes a dFile and returns the hash
func (s *Store) Write(file dFile) (string, error) {
	var hash string
	hash = file.Hash

	if file.Reader != nil {
		buff := new(bytes.Buffer)
		tee := io.TeeReader(file.Reader, buff) // writes to buffer what it reads from R
		addr, err := s.CreateAddress(tee)
		if err != nil {
			return "", err
		}
		log.Println(buff.Bytes())

		fileLock := s.getLock(addr.FullPath())
		defer s.releaseLock(addr.FullPath())
		fileLock.Lock()
		defer fileLock.Unlock()

		hash, err = s.writeStream(buff, false)
		if err != nil {
			return "", err
		}
		if hash != file.Hash {
			return "", fmt.Errorf("file hash mismatch expected %s got %s", file.Hash, hash)
		}
	}
	// TODO: Need to write the metadata regardless lol
	// node does not have the file, we will just write a .metadata file in the expected directory
	metaAddr, err := s.GetAddress(file.Hash)
	if err != nil {
		return "", err
	}

	metadataLock := s.getLock(metaAddr.MetadataPath())
	defer s.releaseLock(metaAddr.MetadataPath())
	metadataLock.Lock()
	defer metadataLock.Unlock()
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(file.dFileMetadata); err != nil {
		return "", err
	}
	if _, err := s.writeStream(&buf, true, metaAddr.MetadataPath()); err != nil {
		return "", err
	}
	return hash, nil

}

// writeStream writes a file into our CAS.
func (s *Store) writeStream(r io.Reader, isMeta bool, forcedPath ...string) (string, error) {
	var metadata_path string
	if isMeta {
		if len(forcedPath) == 0 {
			return "", fmt.Errorf("cannot write metadata without providing path")
		}
		if len(forcedPath) > 1 {
			return "", fmt.Errorf("cannot provide multiple paths")
		}
		metadata_path = forcedPath[0]
	}
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
	var fullpath string
	if isMeta {
		fullpath = metadata_path
	} else {
		fullpath = address.FullPath()
	}
	fmt.Printf("writing path %s", fullpath)
	f, err := os.Create(fullpath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	n, err := io.Copy(f, &buf2)
	if err != nil {
		return "", err
	}

	fmt.Printf("wrote %d bytes to disk, %s\n", n, fullpath)
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
func (f *FileAddress) MetadataPath() string {
	return f.PathName + "/" + dFILE_METADATA
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
