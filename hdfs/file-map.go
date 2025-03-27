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
	Mu           *sync.RWMutex
	Files        map[string]*FileEntry
	DeletedFiles map[string]*DeletedFileEntry // Key is composite of hash + generation stamp
	SnapshotPath string
	// Configuration options
	GCInterval      time.Duration
	RetentionPeriod time.Duration
}

func NewFileMap(snapshotPath string) *FileMap {
	loadedMap, err := Load(snapshotPath)
	if err != nil {
		log.Print("err loading filemap from path, defaulting to empty filemap\n")
		fileMap := &FileMap{
			Mu:              &sync.RWMutex{},
			Files:           make(map[string]*FileEntry),
			DeletedFiles:    make(map[string]*DeletedFileEntry),
			SnapshotPath:    snapshotPath,
			GCInterval:      1 * time.Hour,
			RetentionPeriod: 24 * time.Hour,
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

// Record will Record a file entry into the FileMap. Will merge if an existing key exists
// Record will automatically ignore any previously files
func (fileMap *FileMap) Record(file *proto.FileInfo) {
	fileMap.Mu.Lock()
	defer fileMap.Mu.Unlock()

	// Check if this exact file (hash + generation stamp) was previously deleted
	if fileMap.IsDeleted(file.Hash, file.GenerationStamp) {
		return
	}

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
	fileMap.Mu.Lock()
	defer fileMap.Mu.Unlock()

	if fileMap.Has(file.Hash) == nil {
		return fmt.Errorf("cannot add replica to non-existent file")
	}

	entry := fileMap.Files[file.Hash]

	if entry.Replicas == nil {
		entry.Replicas = make(map[uint64]interface{})
	}

	entry.Replicas[nodeId] = struct{}{}

	return nil
}

func (fileMap *FileMap) RemoveReplica(file *proto.FileInfo, nodeId uint64) error {
	fileMap.Mu.Lock()
	defer fileMap.Mu.Unlock()
	if fileMap.Has(file.Hash) == nil {
		return fmt.Errorf("cannot remove replica to non-existent file")
	}
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
	filemap.Mu.RLock()
	defer filemap.Mu.RUnlock()
	if entry, exist := filemap.Files[hash]; !exist {
		return nil
	} else {
		return entry
	}
}

// Delete deletes a file.
func (fileMap *FileMap) Delete(hash string) *FileEntry {
	fileMap.Mu.Lock()
	defer fileMap.Mu.Unlock()

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
	fileMap.Mu.RLock()
	defer fileMap.Mu.RUnlock()

	key := makeDeletedFileKey(hash, generationStamp)
	_, exists := fileMap.DeletedFiles[key]
	return exists
}

// Snapshot saves the map to disk encoding it with GOB
func (filemap *FileMap) Snapshot() error {
	filemap.Mu.RLock()
	defer filemap.Mu.RUnlock()

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

// Load laods a Filemap from disk using GOB
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
	if filemap.Mu == nil {
		filemap.Mu = &sync.RWMutex{}
	}

	return &filemap, nil
}

func (fileMap *FileMap) runGarbageCollection() {
	fileMap.Mu.Lock()
	defer fileMap.Mu.Unlock()

	now := time.Now()
	deleteCount := 0

	for key, deletedFile := range fileMap.DeletedFiles {
		// If the file has been deleted for longer than the retention period, remove it
		if now.Sub(deletedFile.DeletedAt) > fileMap.RetentionPeriod {
			delete(fileMap.DeletedFiles, key)
			deleteCount++
		}
	}

	if deleteCount > 0 {
		log.Printf("Garbage collection removed %d expired deletion records\n", deleteCount)

		// Take a snapshot after GC
		// Note: Snapshot method doesn't need changes since it accesses the map via the receiver
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
