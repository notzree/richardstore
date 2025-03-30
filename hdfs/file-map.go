package hdfs

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/notzree/richardstore/proto"
)

func init() {
	// Register custom types with gob
	gob.Register(&FileEntry{})
	gob.Register(&DeletedFileEntry{})
	gob.Register(map[uint64]interface{}{})
	gob.Register(struct{}{}) // Register the empty struct used as a set value
}

type FileEntry struct {
	*proto.FileInfo
	Replicas map[uint64]interface{} // set containing list of data node ids that have the file
}

type DeletedFileEntry struct {
	Hash            string
	GenerationStamp uint64
	DeletedAt       time.Time
}

// Create a composite key that combines hash and generation stamp
func makeDeletedFileKey(hash string, generationStamp uint64) string {
	return fmt.Sprintf("%s:%d", hash, generationStamp)
}

func NewFileEntry(fi *proto.FileInfo) *FileEntry {
	return &FileEntry{
		FileInfo: &proto.FileInfo{
			Hash:                 fi.Hash,
			Size:                 fi.Size,
			ModificationStamp:    fi.ModificationStamp,
			MinReplicationFactor: fi.MinReplicationFactor,
			GenerationStamp:      fi.GenerationStamp,
		},
		Replicas: make(map[uint64]interface{}),
	}
}

type FileMap struct {
	mu           *sync.RWMutex
	Files        map[string]*FileEntry
	DeletedFiles map[string]*DeletedFileEntry // Key is composite of hash + generation stamp
	SnapshotPath string
	// Configuration options
	GCInterval      time.Duration
	RetentionPeriod time.Duration
}

func NewFileMap(snapshotPath string, garbageCollectionInterval time.Duration, retentionPeriod time.Duration) *FileMap {
	loadedMap, err := Load(snapshotPath)
	if err != nil {
		log.Print("err loading filemap from path, defaulting to empty filemap\n")
		fileMap := &FileMap{
			mu:              &sync.RWMutex{},
			Files:           make(map[string]*FileEntry),
			DeletedFiles:    make(map[string]*DeletedFileEntry),
			SnapshotPath:    snapshotPath,
			GCInterval:      garbageCollectionInterval,
			RetentionPeriod: retentionPeriod,
		}

		// Start the garbage collection routine
		go fileMap.garbageCollectionRoutine()
		return fileMap
	}

	if loadedMap.DeletedFiles == nil {
		loadedMap.DeletedFiles = make(map[string]*DeletedFileEntry)
	}
	go loadedMap.garbageCollectionRoutine()

	return loadedMap
}

func (fileMap *FileMap) GetFilesArray() []*FileEntry {
	fileMap.mu.RLock()
	defer fileMap.mu.RUnlock()

	// Create a slice with capacity equal to the number of files
	files := make([]*FileEntry, 0, len(fileMap.Files))

	// Populate the slice with deep copies of each entry
	for _, entry := range fileMap.Files {
		// Create a deep copy of each entry
		newEntry := NewFileEntry(entry.FileInfo)

		// Copy replica information
		for nodeId := range entry.Replicas {
			newEntry.Replicas[nodeId] = struct{}{}
		}

		files = append(files, newEntry)
	}

	return files
}

// Record will Record a file entry into the FileMap. Will merge if an existing key exists
// Record will automatically ignore any previously files
func (fileMap *FileMap) Record(file *proto.FileInfo) {
	// Check if this exact file (hash + generation stamp) was previously deleted
	if fileMap.IsDeleted(file.Hash, file.GenerationStamp) {
		log.Printf("Recorded a file that has already been deleted, ignoring...")
		return
	}
	// isDeleted locks mutex so we acquire lock after
	fileMap.mu.Lock()
	defer fileMap.mu.Unlock()

	// File doesnt exist
	if _, exist := fileMap.Files[file.Hash]; !exist {
		fileMap.Files[file.Hash] = NewFileEntry(file)
		return
	}

	// File exists, check if incoming version is newer
	prev_entry := fileMap.Files[file.Hash]
	if prev_entry.ModificationStamp > file.ModificationStamp {
		// our existing version is newer
		return
	}

	// Update FileInfo directly - no need for reassignment
	fileMap.Files[file.Hash].FileInfo = file
}

func (fileMap *FileMap) AddReplica(file *proto.FileInfo, nodeId uint64) error {
	if fileMap.Has(file.Hash) == nil {
		return fmt.Errorf("cannot add replica to non-existent file")
	}
	fileMap.mu.Lock()
	defer fileMap.mu.Unlock()

	entry := fileMap.Files[file.Hash]

	if entry.Replicas == nil {
		entry.Replicas = make(map[uint64]interface{})
	}

	entry.Replicas[nodeId] = struct{}{}

	return nil
}

func (fileMap *FileMap) RemoveReplica(file *proto.FileInfo, nodeId uint64) error {
	if fileMap.Has(file.Hash) == nil {
		return fmt.Errorf("cannot remove replica to non-existent file")
	}

	fileMap.mu.Lock()
	defer fileMap.mu.Unlock()
	entry := fileMap.Files[file.Hash]
	if entry.Replicas == nil {
		// Already no replicas
		return nil
	}
	delete(entry.Replicas, nodeId)
	return nil
}

// threadsafe operation to check if a file with a given hash exists
func (filemap *FileMap) Has(hash string) *FileEntry {
	filemap.mu.RLock()
	defer filemap.mu.RUnlock()
	if entry, exist := filemap.Files[hash]; !exist {
		return nil
	} else {
		return entry
	}
}

// Delete deletes a file.
func (fileMap *FileMap) Delete(hash string) *FileEntry {
	fileMap.mu.Lock()
	defer fileMap.mu.Unlock()

	file, exist := fileMap.Files[hash]
	if !exist {
		return nil
	}

	// Record the deletion with both hash and generation stamp
	key := makeDeletedFileKey(hash, file.GenerationStamp)
	fileMap.DeletedFiles[key] = &DeletedFileEntry{
		Hash:            hash,
		GenerationStamp: file.GenerationStamp,
		DeletedAt:       time.Now(),
	}

	delete(fileMap.Files, hash)
	return file
}

// Is deleted will check if a given hash + generation stamp has been deleted or not.
func (fileMap *FileMap) IsDeleted(hash string, generationStamp uint64) bool {
	fileMap.mu.RLock()
	defer fileMap.mu.RUnlock()

	key := makeDeletedFileKey(hash, generationStamp)
	_, exists := fileMap.DeletedFiles[key]
	return exists
}

// Snapshot saves the map to disk encoding it with GOB
func (filemap *FileMap) Snapshot() error {
	filemap.mu.RLock()
	defer filemap.mu.RUnlock()

	file, err := os.Create(filemap.SnapshotPath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(filemap)
	if err != nil {
		return err
	}
	return nil
}

// Load loads a Filemap from disk using GOB
func Load(path string) (*FileMap, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var filemap FileMap
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&filemap)
	if err != nil {
		return nil, err
	}

	// Initialize mutex if it's nil after loading
	if filemap.mu == nil {
		filemap.mu = &sync.RWMutex{}
	}
	if filemap.DeletedFiles == nil {
		filemap.DeletedFiles = make(map[string]*DeletedFileEntry)
	}

	return &filemap, nil
}

func (fileMap *FileMap) runGarbageCollection() {
	fileMap.mu.Lock()

	now := time.Now()
	deleteCount := 0

	for key, deletedFile := range fileMap.DeletedFiles {
		// If the file has been deleted for longer than the retention period, remove it
		if now.Sub(deletedFile.DeletedAt) > fileMap.RetentionPeriod {
			delete(fileMap.DeletedFiles, key)
			deleteCount++
		}
	}
	fileMap.mu.Unlock()
	if deleteCount > 0 {
		log.Printf("Garbage collection removed %d expired deletion records\n", deleteCount)

		// Take a snapshot after GC
		if err := fileMap.Snapshot(); err != nil {
			log.Printf("Failed to create snapshot after garbage collection: %v\n", err)
		}
	}
}

func (fileMap *FileMap) garbageCollectionRoutine() {
	ticker := time.NewTicker(fileMap.GCInterval)
	defer ticker.Stop()

	for range ticker.C {
		fileMap.runGarbageCollection()
	}
}
