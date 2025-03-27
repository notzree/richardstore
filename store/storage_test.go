package store

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
		BlockSize: BLOCKSIZE,
		Root:      "root",
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
		BlockSize: 5,
		Root:      "root",
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
		BlockSize: 5,
		Root:      tmpDir,
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

func TestStoreSizeTracking(t *testing.T) {
	// Create temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "store-size-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Test with explicit capacity
	t.Run("Explicit capacity", func(t *testing.T) {
		capacity := uint64(1024 * 100) // 100KB
		store := NewStore(StoreOpts{
			BlockSize: 5,
			Root:      tmpDir + "/explicit",
			Capacity:  capacity,
		})
		defer teardown(t, store)

		// Verify initial state
		assert.Equal(t, capacity, store.Capacity, "Store should have the specified capacity")
		assert.Equal(t, uint64(0), store.GetUsedSize(), "Initial used size should be 0")
		assert.Equal(t, capacity, store.GetAvailableSpace(), "Initial available space should equal capacity")

		// Write a file and check size tracking
		data := make([]byte, 1000) // 1KB
		hash, err := store.Write(bytes.NewReader(data))
		assert.NoError(t, err)

		assert.Equal(t, uint64(1000), store.GetUsedSize(), "Used size should be 1000 bytes")
		assert.Equal(t, capacity-1000, store.GetAvailableSpace(), "Available space should be reduced by 1000 bytes")

		// Write another file
		data2 := make([]byte, 2000) // 2KB
		hash2, err := store.Write(bytes.NewReader(data2))
		assert.NoError(t, err)

		assert.Equal(t, uint64(3000), store.GetUsedSize(), "Used size should be 3000 bytes")
		assert.Equal(t, capacity-3000, store.GetAvailableSpace(), "Available space should be capacity minus 3000 bytes")

		// Delete a file and check size tracking
		err = store.Delete(hash)
		assert.NoError(t, err)

		assert.Equal(t, uint64(2000), store.GetUsedSize(), "Used size should be 2000 bytes after deletion")
		assert.Equal(t, capacity-2000, store.GetAvailableSpace(), "Available space should be capacity minus 2000 bytes")

		// Delete second file
		err = store.Delete(hash2)
		assert.NoError(t, err)

		assert.Equal(t, uint64(0), store.GetUsedSize(), "Used size should be 0 after all deletions")
		assert.Equal(t, capacity, store.GetAvailableSpace(), "Available space should equal capacity after all deletions")
	})

	// Test auto capacity from filesystem
	t.Run("Auto capacity", func(t *testing.T) {
		store := NewStore(StoreOpts{
			BlockSize: 5,
			Root:      tmpDir + "/auto",
			Capacity:  0, // Auto-detect
		})
		defer teardown(t, store)

		// Capacity should be non-zero
		assert.Greater(t, store.Capacity, uint64(0), "Auto capacity should be greater than 0")

		// Write a file and check size tracking
		data := make([]byte, 1000) // 1KB
		_, err := store.Write(bytes.NewReader(data))
		assert.NoError(t, err)

		assert.Equal(t, uint64(1000), store.GetUsedSize(), "Used size should be 1000 bytes")
	})

	// Test capacity limits
	t.Run("Capacity limits", func(t *testing.T) {
		capacity := uint64(5000) // 5KB limit
		store := NewStore(StoreOpts{
			BlockSize: 5,
			Root:      tmpDir + "/limit",
			Capacity:  capacity,
		})
		defer teardown(t, store)

		// Write a 3KB file - should succeed
		data1 := make([]byte, 3000)
		_, err := store.Write(bytes.NewReader(data1))
		assert.NoError(t, err)
		assert.Equal(t, uint64(3000), store.GetUsedSize())

		// Write a 3KB file - should fail (exceed capacity)
		data2 := make([]byte, 3000)
		_, err = store.Write(bytes.NewReader(data2))
		assert.Error(t, err, "Write should fail when exceeding capacity")
		assert.Contains(t, err.Error(), "insufficient space")

		// Used size should still be 3KB (the second write failed)
		assert.Equal(t, uint64(3000), store.GetUsedSize())
	})

	// Test file replacement (overwrite)
	t.Run("File replacement", func(t *testing.T) {
		store := NewStore(StoreOpts{
			BlockSize: 5,
			Root:      tmpDir + "/replace",
		})
		defer teardown(t, store)

		// Create a deterministic hash by using the same content repeatedly
		content := []byte("test content")
		h := sha256.New()
		h.Write(content)
		expectedHash := hex.EncodeToString(h.Sum(nil))

		// First write
		hash1, err := store.Write(bytes.NewReader(content))
		assert.NoError(t, err)
		assert.Equal(t, expectedHash, hash1)
		initialSize := store.GetUsedSize()

		// Second write of the same content (should replace, not add to size)
		hash2, err := store.Write(bytes.NewReader(content))
		assert.NoError(t, err)
		assert.Equal(t, expectedHash, hash2)
		assert.Equal(t, initialSize, store.GetUsedSize(), "Size should not increase when overwriting the same file")

		// Write larger version of same content
		largerContent := append(content, []byte(" with additional data")...)
		h = sha256.New()
		h.Write(largerContent)
		largerHash := hex.EncodeToString(h.Sum(nil))

		hash3, err := store.Write(bytes.NewReader(largerContent))
		assert.NoError(t, err)
		assert.Equal(t, largerHash, hash3)
		assert.Greater(t, store.GetUsedSize(), initialSize, "Size should increase when writing larger file")

		// Write smaller version with same hash as original
		smallerContent := []byte("test")
		h = sha256.New()
		h.Write(smallerContent)
		smallerHash := hex.EncodeToString(h.Sum(nil))

		hash4, err := store.Write(bytes.NewReader(smallerContent))
		assert.NoError(t, err)
		assert.Equal(t, smallerHash, hash4)

		// Clear test
		err = store.Clear()
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), store.GetUsedSize(), "Size should be 0 after Clear()")
	})

	// Test edge cases
	t.Run("Edge cases", func(t *testing.T) {
		store := NewStore(StoreOpts{
			BlockSize: 5,
			Root:      tmpDir + "/edge",
			Capacity:  1000,
		})
		defer teardown(t, store)

		// Write a file
		data := make([]byte, 100)
		hash, err := store.Write(bytes.NewReader(data))
		assert.NoError(t, err)

		// Manually corrupt size tracking to test underflow protection
		store.sizeMu.Lock()
		store.usedSize = 50 // Less than file size to test underflow protection
		store.sizeMu.Unlock()

		// Delete should not underflow
		err = store.Delete(hash)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), store.GetUsedSize(), "Size should be 0 and not underflow after delete")

		// Test recalculation of used size on store creation
		// First create some files directly (bypassing size tracking)
		subDir := tmpDir + "/recalc"
		os.MkdirAll(subDir, 0755)

		// Create a few files with known sizes
		file1 := subDir + "/file1"
		file2 := subDir + "/file2"
		os.WriteFile(file1, make([]byte, 200), 0644)
		os.WriteFile(file2, make([]byte, 300), 0644)

		// Create a new store pointing to this directory, it should calculate the correct size
		recalcStore := NewStore(StoreOpts{
			BlockSize: 5,
			Root:      subDir,
		})

		// Size should be the sum of the two files (allowing for potential indexing files)
		size := recalcStore.GetUsedSize()
		assert.GreaterOrEqual(t, size, uint64(500), "Recalculated size should include existing files")
	})
}

func teardown(t *testing.T, store *Store) {
	if err := store.Clear(); err != nil {
		t.Error(err)
	}
}
