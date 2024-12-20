package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// Snapshot Storer will store a snapshot of a struct
type SnapshotStorer[T any] interface {
	Persist(data T) error
	GetMostRecent() (*T, error)
}

// Will use GOB to enc/decode the data and save a snapshot of the struct to the data.
// Will only save public struct fields!!!
type FsSnapShotStore[T any] struct {
	// path to file to write
	filepath string

	mu sync.Mutex
}

func NewFsSnapShotStore[T any](fp string) *FsSnapShotStore[T] {
	dir := filepath.Dir(fp)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("error creating directory: %s", err)
		panic(err)
	}
	return &FsSnapShotStore[T]{
		filepath: fp,
		mu:       sync.Mutex{},
	}
}

func (s *FsSnapShotStore[T]) Persist(data T) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(data); err != nil {
		return err
	}
	tmpFile := s.filepath + ".tmp"
	if err := os.WriteFile(tmpFile, buf.Bytes(), 0644); err != nil {
		return err
	}
	return os.Rename(tmpFile, s.filepath)
}

func (s *FsSnapShotStore[T]) GetMostRecent() (*T, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	file, err := os.Open(s.filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	dec := gob.NewDecoder(file)
	var data T
	if err := dec.Decode(&data); err != nil {
		return nil, err
	}
	return &data, nil
}
