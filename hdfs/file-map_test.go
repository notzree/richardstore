package hdfs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/notzree/richardstore/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create a temporary file for snapshot tests
func createTempFile(t *testing.T) string {
	tempDir, err := os.MkdirTemp("", "filemap-test")
	require.NoError(t, err)

	// Register cleanup function
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	return filepath.Join(tempDir, "filemap.snapshot")
}

// Helper to create a FileInfo for testing
func createTestFileInfo(hash string, size uint64, modStamp uint64, genStamp uint64, minReplica float32) *proto.FileInfo {
	return &proto.FileInfo{
		Hash:                 hash,
		Size:                 size,
		ModificationStamp:    modStamp,
		GenerationStamp:      genStamp,
		MinReplicationFactor: minReplica,
	}
}

func TestNewFileMap(t *testing.T) {
	tempFile := createTempFile(t)

	t.Run("creates new file map when snapshot doesn't exist", func(t *testing.T) {
		fileMap := NewFileMap(tempFile, 1*time.Hour, 24*time.Hour)

		assert.NotNil(t, fileMap)
		assert.NotNil(t, fileMap.mu)
		assert.NotNil(t, fileMap.Files)
		assert.NotNil(t, fileMap.DeletedFiles)
		assert.Equal(t, tempFile, fileMap.SnapshotPath)
		assert.Equal(t, 1*time.Hour, fileMap.GCInterval)
		assert.Equal(t, 24*time.Hour, fileMap.RetentionPeriod)
	})

	t.Run("loads existing file map from snapshot", func(t *testing.T) {
		// First create and save a file map
		fileMap := NewFileMap(tempFile, 1*time.Hour, 24*time.Hour)
		fileInfo := createTestFileInfo("hash1", 100, 1000, 1, 3)
		fileMap.Record(fileInfo)
		err := fileMap.Snapshot()
		require.NoError(t, err)

		// Now load it
		loadedMap := NewFileMap(tempFile, 1*time.Hour, 24*time.Hour)

		assert.NotNil(t, loadedMap)
		assert.Contains(t, loadedMap.Files, "hash1")
	})
}

func TestRecordFile(t *testing.T) {
	fileMap := NewFileMap(createTempFile(t), 1*time.Hour, 24*time.Hour)

	t.Run("records new file", func(t *testing.T) {
		fileInfo := createTestFileInfo("hash1", 100, 1000, 1, 3)
		fileMap.Record(fileInfo)

		entry := fileMap.Has("hash1")
		assert.NotNil(t, entry)
		assert.Equal(t, fileInfo.Hash, entry.Hash)
		assert.Equal(t, fileInfo.Size, entry.Size)
	})

	t.Run("ignores older version of file", func(t *testing.T) {
		// Add initial file
		fileInfo := createTestFileInfo("hash2", 100, 2000, 1, 3)
		fileMap.Record(fileInfo)

		// Try to record older version
		olderInfo := createTestFileInfo("hash2", 50, 1000, 1, 3)
		fileMap.Record(olderInfo)

		// Verify the newer version is still there
		entry := fileMap.Has("hash2")
		assert.NotNil(t, entry)
		assert.Equal(t, uint64(100), entry.Size)
		assert.Equal(t, uint64(2000), entry.ModificationStamp)
	})

	t.Run("updates to newer version of file", func(t *testing.T) {
		// Add initial file
		fileInfo := createTestFileInfo("hash3", 100, 1000, 1, 3)
		fileMap.Record(fileInfo)

		// Record newer version
		newerInfo := createTestFileInfo("hash3", 200, 2000, 2, 3)
		fileMap.Record(newerInfo)

		// Verify it was updated
		entry := fileMap.Has("hash3")
		assert.NotNil(t, entry)
		assert.Equal(t, uint64(200), entry.Size)
		assert.Equal(t, uint64(2000), entry.ModificationStamp)
		assert.Equal(t, uint64(2), entry.GenerationStamp)
	})

	t.Run("ignores deleted files", func(t *testing.T) {
		// Add file and then delete it
		fileInfo := createTestFileInfo("hash4", 100, 1000, 1, 3)
		fileMap.Record(fileInfo)
		fileMap.Delete("hash4")

		// Try to add same file again (same hash and generation stamp)
		sameInfo := createTestFileInfo("hash4", 100, 1000, 1, 3)
		fileMap.Record(sameInfo)

		// Verify it wasn't re-added
		assert.Nil(t, fileMap.Has("hash4"))

		// Try with new generation stamp
		newerGen := createTestFileInfo("hash4", 100, 1000, 2, 3)
		fileMap.Record(newerGen)

		// This one should be added
		entry := fileMap.Has("hash4")
		assert.NotNil(t, entry)
		assert.Equal(t, uint64(2), entry.GenerationStamp)
	})
}

func TestReplicaManagement(t *testing.T) {
	fileMap := NewFileMap(createTempFile(t), 1*time.Hour, 24*time.Hour)
	fileInfo := createTestFileInfo("hash1", 100, 1000, 1, 3)
	fileMap.Record(fileInfo)

	t.Run("adds replica", func(t *testing.T) {
		err := fileMap.AddReplica(fileInfo, 42)
		assert.NoError(t, err)

		entry := fileMap.Has("hash1")
		assert.NotNil(t, entry)
		assert.Contains(t, entry.Replicas, uint64(42))
	})

	t.Run("fails to add replica to non-existent file", func(t *testing.T) {
		nonExistentFile := createTestFileInfo("doesnotexist", 100, 1000, 1, 3)
		err := fileMap.AddReplica(nonExistentFile, 99)
		assert.Error(t, err)
	})

	t.Run("removes replica", func(t *testing.T) {
		// Add replica first
		fileMap.AddReplica(fileInfo, 43)

		// Now remove it
		err := fileMap.RemoveReplica(fileInfo, 43)
		assert.NoError(t, err)

		entry := fileMap.Has("hash1")
		assert.NotNil(t, entry)
		assert.NotContains(t, entry.Replicas, uint64(43))
	})

	t.Run("handles removing non-existent replica", func(t *testing.T) {
		err := fileMap.RemoveReplica(fileInfo, 9999)
		assert.NoError(t, err)
	})
}

func TestDeletion(t *testing.T) {
	fileMap := NewFileMap(createTempFile(t), 1*time.Hour, 24*time.Hour)

	t.Run("deletes existing file", func(t *testing.T) {
		fileInfo := createTestFileInfo("hash1", 100, 1000, 1, 3)
		fileMap.Record(fileInfo)

		deletedEntry := fileMap.Delete("hash1")
		assert.NotNil(t, deletedEntry)
		assert.Equal(t, "hash1", deletedEntry.Hash)

		// Verify it's gone from Files
		assert.Nil(t, fileMap.Has("hash1"))

		// Verify it's in DeletedFiles
		assert.True(t, fileMap.IsDeleted("hash1", 1))
	})

	t.Run("returns nil for non-existent file", func(t *testing.T) {
		deletedEntry := fileMap.Delete("doesnotexist")
		assert.Nil(t, deletedEntry)
	})
}

func TestGetFilesArray(t *testing.T) {
	fileMap := NewFileMap(createTempFile(t), 1*time.Hour, 24*time.Hour)

	// Add a few files
	fileMap.Record(createTestFileInfo("hash1", 100, 1000, 1, 3))
	fileMap.Record(createTestFileInfo("hash2", 200, 2000, 1, 3))

	// Add replicas
	fileMap.AddReplica(createTestFileInfo("hash1", 0, 0, 0, 0), 1)
	fileMap.AddReplica(createTestFileInfo("hash1", 0, 0, 0, 0), 2)

	t.Run("returns all files as array", func(t *testing.T) {
		files := fileMap.GetFilesArray()

		assert.Len(t, files, 2)

		// Find hash1 entry
		var hash1Entry *FileEntry
		var hash2Entry *FileEntry
		for _, entry := range files {
			if entry.Hash == "hash1" {
				hash1Entry = entry
			} else if entry.Hash == "hash2" {
				hash2Entry = entry
			}
		}

		assert.NotNil(t, hash1Entry)
		assert.NotNil(t, hash2Entry)

		assert.Equal(t, uint64(100), hash1Entry.Size)
		assert.Equal(t, uint64(200), hash2Entry.Size)

		// Check replicas
		assert.Len(t, hash1Entry.Replicas, 2)
		assert.Contains(t, hash1Entry.Replicas, uint64(1))
		assert.Contains(t, hash1Entry.Replicas, uint64(2))
	})
}

func TestSnapshotAndLoad(t *testing.T) {
	tempFile := createTempFile(t)
	fileMap := NewFileMap(tempFile, 1*time.Hour, 24*time.Hour)

	// Add files and replicas
	fileMap.Record(createTestFileInfo("hash1", 100, 1000, 1, 3))
	fileMap.Record(createTestFileInfo("hash2", 200, 2000, 1, 3))
	fileMap.AddReplica(createTestFileInfo("hash1", 0, 0, 0, 0), 1)

	// Delete a file
	fileMap.Delete("hash2")

	t.Run("saves and loads correctly", func(t *testing.T) {
		// Save
		err := fileMap.Snapshot()
		assert.NoError(t, err)

		// Load
		loadedMap, err := Load(tempFile)
		assert.NoError(t, err)

		// Check loaded map
		assert.NotNil(t, loadedMap)
		assert.NotNil(t, loadedMap.Files["hash1"])
		assert.Equal(t, uint64(100), loadedMap.Files["hash1"].Size)

		// Check replicas
		assert.Contains(t, loadedMap.Files["hash1"].Replicas, uint64(1))

		// Check deleted files
		key := makeDeletedFileKey("hash2", 1)
		assert.Contains(t, loadedMap.DeletedFiles, key)
		assert.Equal(t, "hash2", loadedMap.DeletedFiles[key].Hash)
	})
}

func TestGarbageCollection(t *testing.T) {
	fileMap := NewFileMap(createTempFile(t), 100*time.Millisecond, 100*time.Millisecond)

	// Add a file then delete it
	fileMap.Record(createTestFileInfo("hash1", 100, 1000, 1, 3))
	fileMap.Delete("hash1")

	t.Run("removes old deletions", func(t *testing.T) {
		// Verify deletion record exists
		assert.True(t, fileMap.IsDeleted("hash1", 1))

		// Wait for retention period to pass
		time.Sleep(200 * time.Millisecond)

		// Manually run GC (rather than waiting for the ticker)
		fileMap.runGarbageCollection()
		// Verify deletion record is gone
		assert.False(t, fileMap.IsDeleted("hash1", 1))
	})
}

func TestIsDeleted(t *testing.T) {
	fileMap := NewFileMap(createTempFile(t), 1*time.Hour, 24*time.Hour)

	// Add files with different generation stamps
	file1 := createTestFileInfo("samehash", 100, 1000, 1, 3)
	file2 := createTestFileInfo("samehash", 200, 2000, 2, 3)

	fileMap.Record(file1)
	fileMap.Record(file2)

	// Delete only the first version
	fileMap.Delete("samehash")

	t.Run("checks deletion with generation stamp", func(t *testing.T) {
		// Should be deleted
		assert.True(t, fileMap.IsDeleted("samehash", 2))

		// Re-add with new generation stamp
		file3 := createTestFileInfo("samehash", 300, 3000, 3, 3)
		fileMap.Record(file3)

		// Previous version still marked as deleted
		assert.True(t, fileMap.IsDeleted("samehash", 2))

		// New version not deleted
		assert.False(t, fileMap.IsDeleted("samehash", 3))
	})
}
