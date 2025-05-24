package datastore

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time" // Добавлен импорт time для таймаута в тесте
)

// TestDb encapsulates all major tests for the Db
func TestDb(t *testing.T) {
	// Основной временный каталог для тестов
	baseTmpDir := t.TempDir()

	t.Run("Put, Get and Persistence", func(t *testing.T) {
		tmpDir := filepath.Join(baseTmpDir, "put_get_persistence")
		db, err := Open(tmpDir, 1024) // maxSegmentSize = 1KB
		if err != nil {
			t.Fatalf("failed to open db: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
		})

		pairs := [][]string{
			{"k1", "v1"},
			{"k2", "v2"},
			{"k3", "v3"},
			{"k2", "v2.1"}, // обновление значения
		}

		// Put and Get
		for _, pair := range pairs {
			key, value := pair[0], pair[1]
			if err := db.Put(key, value); err != nil {
				t.Fatalf("Put failed for key=%s: %v", key, err)
			}
			got, err := db.Get(key)
			if err != nil {
				t.Fatalf("Get failed for key=%s: %v", key, err)
			}
			if got != value {
				t.Errorf("unexpected value for key=%s: got=%s, want=%s", key, got, value)
			}
		}

		// Size increases after writes
		initialSize, err := db.Size()
		if err != nil {
			t.Fatalf("Size() failed: %v", err)
		}
		if err := db.Put("k4", "v4"); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		newSize, err := db.Size()
		if err != nil {
			t.Fatalf("Size() failed: %v", err)
		}
		if newSize <= initialSize {
			t.Errorf("expected size to grow: before=%d, after=%d", initialSize, newSize)
		}

		// Data persists after reopen
		if err := db.Close(); err != nil {
			t.Fatalf("failed to close db: %v", err)
		}

		db, err = Open(tmpDir, 1024) // Reopen in the same directory
		if err != nil {
			t.Fatalf("failed to reopen db: %v", err)
		}
		t.Cleanup(func() { // Add cleanup for the reopened db instance
			_ = db.Close()
		})

		expected := map[string]string{
			"k1": "v1",
			"k2": "v2.1",
			"k3": "v3",
			"k4": "v4",
		}
		for key, want := range expected {
			got, err := db.Get(key)
			if err != nil {
				t.Errorf("Get failed after reopen for key=%s: %v", key, err)
				continue
			}
			if got != want {
				t.Errorf("wrong value after reopen for key=%s: got=%s, want=%s", key, got, want)
			}
		}
	})

	t.Run("Get non-existent key returns ErrNotFound", func(t *testing.T) {
		tmpDir := filepath.Join(baseTmpDir, "err_not_found")
		db, err := Open(tmpDir, 1024)
		if err != nil {
			t.Fatalf("failed to open db: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
		})

		_, err = db.Get("nonExistentKey")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("Automatic segment switching", func(t *testing.T) {
		tmpDir := filepath.Join(baseTmpDir, "segment_switching")
		// Use a very small maxSegmentSize for easy testing
		// An entry "k1":"v1" is about 16 bytes (keyLen=2, valLen=2, total 2+2+12)
		// Set maxSegmentSize to 20 bytes to force new segments frequently
		db, err := Open(tmpDir, 20)
		if err != nil {
			t.Fatalf("failed to open db: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
		})

		initialSegments := len(db.segments)
		if initialSegments != 1 {
			t.Fatalf("expected 1 segment initially, got %d", initialSegments)
		}

		// Write multiple entries to force segment switching
		// Each entry should be around 16 bytes. Max size 20 means each entry likely creates a new segment.
		numEntries := 5
		for i := 0; i < numEntries; i++ {
			key := fmt.Sprintf("key%d", i)
			value := fmt.Sprintf("value%d", i) // Simple value, entry size is 12 + len(key) + len(value)
			if err := db.Put(key, value); err != nil {
				t.Fatalf("Put failed for %s: %v", key, err)
			}
		}

		// We expect more than the initial segment, at least 2 or 3
		if len(db.segments) <= initialSegments {
			t.Errorf("expected number of segments to increase, but it's %d", len(db.segments))
		}
		if len(db.segments) < 3 { // Depending on exact entry size and max, might be 3 or more.
			t.Logf("Warning: Expected at least 3 segments after %d puts with maxSegmentSize %d, got %d. Check entry size vs maxSegmentSize.", numEntries, 20, len(db.segments))
		}

		// Verify all data is still accessible across multiple segments
		for i := 0; i < numEntries; i++ {
			key := fmt.Sprintf("key%d", i)
			expectedValue := fmt.Sprintf("value%d", i)
			got, err := db.Get(key)
			if err != nil {
				t.Fatalf("Get failed for %s: %v", key, err)
			}
			if got != expectedValue {
				t.Errorf("unexpected value for key %s: got %s, want %s", key, got, expectedValue)
			}
		}
	})

	t.Run("Compact operation", func(t *testing.T) {
		tmpDir := filepath.Join(baseTmpDir, "compaction")
		// Use a small maxSegmentSize to quickly create multiple segments
		db, err := Open(tmpDir, 20) // About 20 bytes per entry, forces new segments
		if err != nil {
			t.Fatalf("failed to open db: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
		})

		// Write data to create multiple segments and overwrite keys
		keysToPut := []struct{ key, value string }{
			{"k1", "v1"}, {"k2", "v2"}, // Segment 1
			{"k3", "v3"}, {"k1", "v1_updated"}, // Segment 2 (k1 updated)
			{"k4", "v4"}, {"k2", "v2_final"}, // Segment 3 (k2 updated)
		}

		for _, p := range keysToPut {
			if err := db.Put(p.key, p.value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		initialSegments := len(db.segments)
		if initialSegments < 3 { // Ensure we have enough segments to compact
			t.Fatalf("expected at least 3 segments before compaction, got %d", initialSegments)
		}

		initialSize, err := db.Size()
		if err != nil {
			t.Fatalf("Size failed: %v", err)
		}

		t.Logf("Before compaction: %d segments, total size %d bytes", initialSegments, initialSize)

		// Trigger background compaction
		db.Compact()

		// Wait for the compaction to complete.
		// Using db.compactionWg.Wait() is safe here because it's a sync.WaitGroup,
		// and Compact() adds to it, and the goroutine calls Done() when finished.
		// Add a timeout to prevent test from hanging indefinitely in case of deadlock or bug.
		done := make(chan struct{})
		go func() {
			db.compactionWg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Compaction finished
		case <-time.After(30 * time.Second): // 30 seconds timeout
			t.Fatal("Compaction timed out")
		}

		// After compaction, based on current implementation, all old segments are merged into one new segment 1,
		// and the previously active segment becomes segment 2. So we expect 2 segments.
		if len(db.segments) != 2 {
			t.Errorf("expected 2 segments after compaction, got %d", len(db.segments))
		}

		compactedSize, err := db.Size()
		if err != nil {
			t.Fatalf("Size failed after compact: %v", err)
		}

		if compactedSize >= initialSize {
			t.Errorf("expected size to decrease after compaction, before=%d, after=%d", initialSize, compactedSize)
		}
		t.Logf("After compaction: %d segments, total size %d bytes", len(db.segments), compactedSize)

		// Verify values after compaction - only latest versions should remain
		expected := map[string]string{
			"k1": "v1_updated",
			"k2": "v2_final",
			"k3": "v3",
			"k4": "v4",
		}

		for key, want := range expected {
			got, err := db.Get(key)
			if err != nil {
				t.Errorf("Get failed after compaction for key=%s: %v", key, err)
				continue
			}
			if got != want {
				t.Errorf("wrong value after compaction for key=%s: got=%s, want=%s", key, got, want)
			}
		}

		// Verify persistence after compaction
		if err := db.Close(); err != nil {
			t.Fatalf("failed to close after compaction: %v", err)
		}
		dbReopened, err := Open(tmpDir, 20) // Reopen in the same directory
		if err != nil {
			t.Fatalf("failed to reopen after compaction: %v", err)
		}
		t.Cleanup(func() {
			_ = dbReopened.Close()
		})

		for key, want := range expected {
			got, err := dbReopened.Get(key)
			if err != nil {
				t.Errorf("Get failed after reopen and compaction for key=%s: %v", key, err)
				continue
			}
			if got != want {
				t.Errorf("wrong value after reopen and compaction for key=%s: got=%s, want=%s", key, got, want)
			}
		}
	})

	t.Run("Concurrency", func(t *testing.T) {
		tmpDir := filepath.Join(baseTmpDir, "concurrency")
		db, err := Open(tmpDir, 1024)
		if err != nil {
			t.Fatalf("failed to open db: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
		})

		var wg sync.WaitGroup
		numWorkers := 5        // Количество конкурентных горутин
		numOpsPerWorker := 200 // Количество операций (Put/Get) на каждую горутину

		// Map to store expected final values (protected by mutex)
		expected := make(map[string]string)
		var expectedMu sync.Mutex

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < numOpsPerWorker; j++ {
					key := fmt.Sprintf("ck-%d-%d", workerID, j) // Unique key for Put
					value := fmt.Sprintf("cv-%d-%d", workerID, j)

					// Perform a Put operation
					if err := db.Put(key, value); err != nil {
						t.Errorf("worker %d: Put failed for %s: %v", workerID, key, err)
						return // Exit goroutine on error
					}
					expectedMu.Lock()
					expected[key] = value
					expectedMu.Unlock()

					// Perform a Get operation (maybe for a key written by another worker)
					// Try to get a key that might exist or not
					getKey := fmt.Sprintf("ck-%d-%d", (workerID+1)%numWorkers, j) // Try to read from another worker's range
					_, err := db.Get(getKey)
					if err != nil && !errors.Is(err, ErrNotFound) {
						t.Errorf("worker %d: Get failed for %s: %v", workerID, getKey, err)
						return // Exit goroutine on error
					}
				}
			}(i)
		}
		wg.Wait() // Wait for all goroutines to complete

		// Verify all expected values after concurrent operations
		for key, want := range expected {
			got, err := db.Get(key)
			if err != nil {
				t.Errorf("Get failed after concurrent ops for key=%s: %v", key, err)
				continue
			}
			if got != want {
				t.Errorf("wrong value after concurrent ops for key=%s: got=%s, want=%s", key, got, want)
			}
		}
	})
}
