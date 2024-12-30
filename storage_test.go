package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateAddressFunc(t *testing.T) {
	const BLOCKSIZE = 5
	opts := StoreOpts{
		blockSize: BLOCKSIZE,
		root:      "root",
	}
	s := NewStore(opts)
	content := "momsbestpicture"
	buf := bytes.NewBuffer([]byte(content))
	hash := sha256.New()
	io.Copy(hash, buf)
	hashStr := hex.EncodeToString(hash.Sum(nil))
	fileAddress, err := s.CreateAddress(bytes.NewReader([]byte(content)))
	if err != nil {
		panic(err)
	}
	assert.Equal(t, fileAddress.HashStr, hashStr)
	split := strings.Split(fileAddress.PathName, "/")
	index := 0
	// skipping the last character since it might overflow depending on blocksize
	for _, dir := range split[1 : len(split)-1] { // skip the root directory b/c its always root + / + address
		log.Println(dir)
		assert.Equal(t, len(dir), BLOCKSIZE)
		expectedString := hashStr[index : index+BLOCKSIZE]
		assert.Equal(t, expectedString, dir)
		index += BLOCKSIZE
	}
}

func TestStoreSingleAccess(t *testing.T) {
	opts := StoreOpts{
		blockSize: 5,
		root:      "root",
	}
	s := NewStore(opts)
	defer teardown(t, s)
	data := []byte("cringe nft12222")
	buf := bytes.NewBuffer(data)
	// test write
	hash, err := s.Write(buf)
	if err != nil {
		t.Error(err)
	}
	// test read
	r, err := s.readStream(hash)
	if err != nil {
		t.Error(err)
	}
	exists := s.Has(hash)
	if !exists {
		t.Errorf("expected to have key %s", hash)
	}

	readData, err := io.ReadAll(r)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, readData, data)
	// test delete
	err = s.Delete(hash)
	if err != nil {
		t.Error(err)
	}
}

func TestStoreConcurrency(t *testing.T) {
	// Create temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "store-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewStore(StoreOpts{
		blockSize: 5,
		root:      tmpDir,
	})
	defer teardown(t, store)

	t.Run("Concurrent reads of same file", func(t *testing.T) {
		// First write a file
		data := []byte("test data for concurrent reads")
		reader := bytes.NewReader(data)
		hash, err := store.Write(reader)
		if err != nil {
			t.Fatal(err)
		}

		// Create wait group for goroutines
		var wg sync.WaitGroup
		numReaders := 10

		// Launch multiple concurrent readers
		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				reader, err := store.Read(hash)
				if err != nil {
					t.Errorf("Reader %d failed: %v", id, err)
					return
				}
				readData, err := io.ReadAll(reader)
				if err != nil {
					t.Errorf("Reader %d failed to read: %v", id, err)
					return
				}
				if !bytes.Equal(readData, data) {
					t.Errorf("Reader %d got wrong data", id)
				}
			}(i)
		}
		wg.Wait()
	})

	t.Run("Concurrent writes of different files", func(t *testing.T) {
		var wg sync.WaitGroup
		numWriters := 10
		hashes := make([]string, numWriters)
		var hashMutex sync.Mutex

		// Launch multiple concurrent writers
		for i := 0; i < numWriters; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				data := []byte(fmt.Sprintf("test data %d", id))
				reader := bytes.NewReader(data)
				hash, err := store.Write(reader)
				if err != nil {
					t.Errorf("Writer %d failed: %v", id, err)
					return
				}

				// Safely store the hash
				hashMutex.Lock()
				hashes[id] = hash
				hashMutex.Unlock()

				// Verify the write immediately
				r, err := store.Read(hash)
				if err != nil {
					t.Errorf("Writer %d failed to read back: %v", id, err)
					return
				}
				readData, err := io.ReadAll(r)
				if err != nil {
					t.Errorf("Writer %d failed to read data: %v", id, err)
					return
				}
				if !bytes.Equal(readData, data) {
					t.Errorf("Writer %d: data mismatch", id)
				}
			}(i)
		}
		wg.Wait()

		// Verify all files still exist and are readable
		for i, hash := range hashes {
			reader, err := store.Read(hash)
			if err != nil {
				t.Errorf("Failed to read file %d after all writes: %v", i, err)
				continue
			}
			readData, err := io.ReadAll(reader)
			if err != nil {
				t.Errorf("Failed to read data from file %d after all writes: %v", i, err)
				continue
			}
			expectedData := []byte(fmt.Sprintf("test data %d", i))
			if !bytes.Equal(readData, expectedData) {
				t.Errorf("File %d data mismatch after all writes", i)
			}
		}
	})

	t.Run("Mixed read/write/delete operations", func(t *testing.T) {
		// First write an initial file that will be read concurrently
		initialData := []byte("initial test data")
		reader := bytes.NewReader(initialData)
		initialHash, err := store.Write(reader)
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		numOps := 10
		errChan := make(chan error, numOps*3) // Buffer for potential errors

		// Launch readers for initial file
		for i := 0; i < numOps; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				reader, err := store.Read(initialHash)
				if err != nil {
					errChan <- fmt.Errorf("Reader %d failed: %v", id, err)
					return
				}
				readData, err := io.ReadAll(reader)
				if err != nil {
					errChan <- fmt.Errorf("Reader %d failed to read: %v", id, err)
					return
				}
				if !bytes.Equal(readData, initialData) {
					errChan <- fmt.Errorf("Reader %d got wrong data", id)
				}
			}(i)
		}

		// Launch writers of new files
		for i := 0; i < numOps; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				data := []byte(fmt.Sprintf("concurrent write %d", id))
				reader := bytes.NewReader(data)
				hash, err := store.Write(reader)
				if err != nil {
					errChan <- fmt.Errorf("Writer %d failed: %v", id, err)
					return
				}

				// Try to read it back
				r, err := store.Read(hash)
				if err != nil {
					errChan <- fmt.Errorf("Writer %d failed to read back: %v", id, err)
					return
				}
				readData, err := io.ReadAll(r)
				if err != nil {
					errChan <- fmt.Errorf("Writer %d failed to read data: %v", id, err)
					return
				}
				if !bytes.Equal(readData, data) {
					errChan <- fmt.Errorf("Writer %d: data mismatch", id)
				}

				// Delete the file we just wrote
				if err := store.Delete(hash); err != nil {
					errChan <- fmt.Errorf("Delete %d failed: %v", id, err)
				}
			}(i)
		}

		wg.Wait()
		close(errChan)

		// Check for any errors
		for err := range errChan {
			t.Error(err)
		}

		// Verify initial file is still readable
		r, err := store.Read(initialHash)
		if err != nil {
			t.Fatal(err)
		}
		readData, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(readData, initialData) {
			t.Error("Initial file data corrupted")
		}
	})
}

func teardown(t *testing.T, store *Store) {
	if err := store.Clear(); err != nil {
		t.Error(err)
	}
}
