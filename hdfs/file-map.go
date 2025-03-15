package hdfs

import (
	"encoding/gob"
	"log"
	"os"
	"sync"

	"github.com/notzree/richardstore/proto"
)

type FileEntry struct {
	*proto.FileInfo                        // fields of FileInfo
	Replicas        map[uint64]interface{} // set containing list of data node ids that have the file
}

func NewFileEntry(fi *proto.FileInfo) *FileEntry {
	return &FileEntry{
		FileInfo: &proto.FileInfo{
			Hash:                 fi.Hash,
			Size:                 fi.Size,
			ModificationStamp:    fi.ModificationStamp,
			MinReplicationFactor: fi.MinReplicationFactor,
		},
		Replicas: make(map[uint64]interface{}),
	}
}

type FileMap struct {
	Mu    *sync.RWMutex
	Files map[string]FileEntry

	SnapshotPath string
}

func NewFileMap(path string) *FileMap {
	loadedMap, err := Load(path)
	if err != nil {
		log.Print("err loading filemap from path, defaulting to empty filemap\n")
		return &FileMap{
			Mu:           &sync.RWMutex{},
			Files:        make(map[string]FileEntry),
			SnapshotPath: path,
		}
	}
	return loadedMap
}

// Record will Record a file entry into the FileMap. Will merge if an existing key exists
func (filemap *FileMap) Record(file FileEntry) {
	filemap.Mu.Lock()
	defer filemap.Mu.Unlock()
	if _, exist := filemap.Files[file.Hash]; !exist {
		filemap.Files[file.Hash] = file
		return
	}
	prev_entry := filemap.Files[file.Hash]
	if prev_entry.ModificationStamp > file.ModificationStamp {
		// our existing version is newer
		return
	}
	filemap.Files[file.Hash] = file
}

// threadsafe operation to check if a file with a given hash exists
func (filemap *FileMap) Has(hash string) *FileEntry {
	filemap.Mu.RLock()
	defer filemap.Mu.RUnlock()
	if entry, exist := filemap.Files[hash]; !exist {
		return nil
	} else {
		return &entry
	}
}

// Delete deletes a file. Returns True if file was deleted, false if did not exist
func (filemap *FileMap) Delete(hash string) bool {
	filemap.Mu.Lock()
	defer filemap.Mu.Unlock()
	if _, exist := filemap.Files[hash]; !exist {
		return false
	}
	delete(filemap.Files, hash)
	return true
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
