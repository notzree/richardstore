package store

import (
	"bytes"
	"crypto/sha256"
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

	"golang.org/x/sys/unix"
)

type StoreOpts struct {
	BlockSize int
	Root      string
	// Capacity in bytes, if 0 use all available space
	Capacity uint64
}

// concurrent safe struct to read/write bytes to a CAS file system
type Store struct {
	StoreOpts
	mu       sync.RWMutex // mutex for the locks map
	locks    map[string]*lock
	sizeMu   sync.RWMutex // mutex for size tracking
	usedSize uint64       // total size of all stored files in bytes
}

type lock struct {
	sync.RWMutex     // per-file lock
	refCount     int // num of active operations
}

func NewStore(opts StoreOpts) *Store {
	if opts.Root == "" {
		// If no root is specified, use the current working directory
		cwd, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		opts.Root = cwd
	}

	// Create the root directory if it doesn't exist
	if err := os.MkdirAll(opts.Root, 0755); err != nil {
		panic(fmt.Errorf("failed to create root directory: %w", err))
	}

	store := &Store{
		StoreOpts: opts,
		locks:     make(map[string]*lock),
		usedSize:  0,
	}

	// Set capacity to available space if not explicitly set
	if opts.Capacity == 0 {
		store.Capacity = store.GetAvailableCapacity()
	}

	// Calculate current used size by scanning existing files
	store.calculateUsedSize()

	return store
}

// calculateUsedSize scans all files in the store and updates the usedSize field
func (s *Store) calculateUsedSize() {
	files, err := s.Stat()
	if err != nil {
		log.Printf("Warning: Could not calculate initial used size: %v", err)
		return
	}

	var totalSize uint64
	for _, file := range files {
		info, err := file.Stat()
		if err != nil {
			file.Close()
			continue
		}
		totalSize += uint64(info.Size())
		file.Close()
	}

	s.sizeMu.Lock()
	s.usedSize = totalSize
	s.sizeMu.Unlock()
	log.Printf("Initial store used size: %d bytes", totalSize)
}

// GetUsedSize returns the total size of all files in the store
func (s *Store) GetUsedSize() uint64 {
	s.sizeMu.RLock()
	defer s.sizeMu.RUnlock()
	return s.usedSize
}

// GetAvailableSpace returns the remaining space available for storage
func (s *Store) GetAvailableSpace() uint64 {
	s.sizeMu.RLock()
	defer s.sizeMu.RUnlock()

	if s.usedSize >= s.Capacity {
		return 0
	}
	return s.Capacity - s.usedSize
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
	hash := sha256.New()
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
	directoryDepth := len(hashStr) / s.BlockSize
	paths := make([]string, directoryDepth)
	for i := 0; i < directoryDepth; i++ {
		from, to := i*s.BlockSize, (i*s.BlockSize)+s.BlockSize
		paths[i] = hashStr[from:to]
	}
	nRead := directoryDepth * s.BlockSize
	leftOver := len(hashStr) - nRead

	// TODO: Fix hacky approach currently last directory may be up to 2*blockSize-1 long
	if leftOver != 0 {
		paths[len(paths)-1] = paths[len(paths)-1] + hashStr[nRead:nRead+leftOver]
	}

	joinedPaths := strings.Join(paths, "/")
	pathStr := filepath.Join(s.Root, joinedPaths)
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

	// Get file size before deleting
	fileInfo, err := os.Stat(path.FullPath())
	if err != nil {
		return err
	}
	fileSize := uint64(fileInfo.Size())

	dir := filepath.Dir(path.FullPath())
	fmt.Println("full path:", dir)
	// delete file
	err = os.Remove(path.FullPath())
	if err != nil {
		return err
	}

	// Update size tracking
	s.sizeMu.Lock()
	if s.usedSize >= fileSize {
		s.usedSize -= fileSize
	} else {
		// Safeguard against underflow
		s.usedSize = 0
		log.Printf("Warning: Size tracking inconsistency detected during delete of %s", key)
	}
	s.sizeMu.Unlock()

	// remove empty directories excluding root directory
	for dir != s.Root {
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
func (s *Store) Read(key string) (*os.File, error) {
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
func (s *Store) readStream(key string) (*os.File, error) {
	address, err := s.GetAddress(key)
	if err != nil {
		return nil, err
	}
	return os.Open(address.FullPath())
}

func (s *Store) Write(r io.Reader) (string, error) {
	if err := os.MkdirAll(s.Root, fs.ModePerm); err != nil {
		return "", fmt.Errorf("failed to create root directory: %w", err)
	}

	// Create a temporary buffer to determine the file size before writing
	var buf bytes.Buffer
	size, err := io.Copy(&buf, r)
	if err != nil {
		return "", fmt.Errorf("failed to read data: %w", err)
	}

	// Check if we have enough space
	s.sizeMu.RLock()
	if uint64(size) > s.Capacity-s.usedSize {
		s.sizeMu.RUnlock()
		return "", fmt.Errorf("insufficient space: need %d bytes, have %d bytes available", size, s.Capacity-s.usedSize)
	}
	s.sizeMu.RUnlock()

	tempFile, err := os.CreateTemp(s.Root, "temp-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	tempPath := tempFile.Name()

	// Clean up temp file if anything goes wrong
	defer func() {
		tempFile.Close()
		if err != nil {
			os.Remove(tempPath)
		}
	}()

	h := sha256.New()

	// Reset buffer position
	bufReader := bytes.NewReader(buf.Bytes())

	writer := io.MultiWriter(tempFile, h)
	// Stream data from buffer to MultiWriter which streams to -> tempFile, h
	if _, err = io.Copy(writer, bufReader); err != nil {
		return "", fmt.Errorf("failed to write data: %w", err)
	}

	hashStr := hex.EncodeToString(h.Sum(nil))
	address, err := s.GetAddress(hashStr)
	if err != nil {
		return "", fmt.Errorf("failed to create address: %w", err)
	}

	// Get lock before moving file to final location
	fileLock := s.getLock(address.HashStr)
	defer s.releaseLock(address.HashStr)
	fileLock.Lock()
	defer fileLock.Unlock()

	// Check if file already exists (to avoid double-counting size)
	existingSize := uint64(0)
	if info, err := os.Stat(address.FullPath()); err == nil {
		existingSize = uint64(info.Size())
	}

	// Create directories
	if err := os.MkdirAll(address.PathName, fs.ModePerm); err != nil {
		return "", fmt.Errorf("failed to create directories: %w", err)
	}

	// Close temp file before moving it
	if err = tempFile.Close(); err != nil {
		return "", fmt.Errorf("failed to close temp file: %w", err)
	}

	// Move temp file to final location
	if err = os.Rename(tempPath, address.FullPath()); err != nil {
		return "", fmt.Errorf("failed to move file to final location: %w", err)
	}

	// Update size tracking (only count the net new size if replacing an existing file)
	s.sizeMu.Lock()
	netNewSize := uint64(size)
	if existingSize > 0 {
		if netNewSize > existingSize {
			netNewSize -= existingSize
		} else {
			netNewSize = 0
		}
	}
	s.usedSize += netNewSize
	s.sizeMu.Unlock()

	fmt.Printf("wrote file to disk: %s (size: %d bytes)\n", hashStr, size)
	return hashStr, nil
}

// Clear deletes the root and all subdirectories
func (s *Store) Clear() error {
	err := os.RemoveAll(s.Root)
	if err != nil {
		return err
	}

	// Reset size tracking
	s.sizeMu.Lock()
	s.usedSize = 0
	s.sizeMu.Unlock()

	return nil
}

// Stat opens (DOES NOT close) and returns a list of os.Files that the storer has stored.
func (s *Store) Stat() ([]*os.File, error) {
	if _, err := os.Stat(s.Root); err != nil {
		if os.IsNotExist(err) {
			return make([]*os.File, 0), nil
		} else {
			return nil, err
		}
	}
	heldFiles := make([]*os.File, 0)
	err := filepath.WalkDir(s.Root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			heldFiles = append(heldFiles, file)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return heldFiles, nil
}

type FileAddress struct {
	PathName string
	HashStr  string
}

// FullPath returns the full path to the file including the root directory
func (f *FileAddress) FullPath() string {
	return f.PathName + "/" + f.HashStr
}

func (s *Store) GetAvailableCapacity() uint64 {
	var stat unix.Statfs_t
	err := unix.Statfs(s.Root, &stat)
	if err != nil {
		log.Printf("Warning: Could not get disk stats: %v", err)
		return 1 << 40 // 1TB default for now?
	}
	log.Printf("Bavail: %v, Bsize: %v", stat.Bavail, stat.Bsize)
	return stat.Bavail * uint64(stat.Bsize)
}
