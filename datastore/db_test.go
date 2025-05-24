package datastore

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestDb(t *testing.T) {
	baseTmpDir := t.TempDir()

	t.Run("Put, Get and Persistence", func(t *testing.T) {
		tmpDir := filepath.Join(baseTmpDir, "put_get_persistence")
		db, err := Open(tmpDir, 1024)
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
			{"k2", "v2.1"},
		}

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

		if err := db.Close(); err != nil {
			t.Fatalf("failed to close db: %v", err)
		}

		db, err = Open(tmpDir, 1024)
		if err != nil {
			t.Fatalf("failed to reopen db: %v", err)
		}
		t.Cleanup(func() {
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

		numEntries := 5
		for i := 0; i < numEntries; i++ {
			key := fmt.Sprintf("key%d", i)
			value := fmt.Sprintf("value%d", i)
			if err := db.Put(key, value); err != nil {
				t.Fatalf("Put failed for %s: %v", key, err)
			}
		}

		if len(db.segments) <= initialSegments {
			t.Errorf("expected number of segments to increase, but it's %d", len(db.segments))
		}
		if len(db.segments) < 3 {
			t.Logf("Warning: Expected at least 3 segments after %d puts with maxSegmentSize %d, got %d. Check entry size vs maxSegmentSize.", numEntries, 20, len(db.segments))
		}

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
		db, err := Open(tmpDir, 20)
		if err != nil {
			t.Fatalf("failed to open db: %v", err)
		}
		t.Cleanup(func() {
			_ = db.Close()
		})

		keysToPut := []struct{ key, value string }{
			{"k1", "v1"}, {"k2", "v2"},
			{"k3", "v3"}, {"k1", "v1_updated"},
			{"k4", "v4"}, {"k2", "v2_final"},
		}

		for _, p := range keysToPut {
			if err := db.Put(p.key, p.value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		initialSegments := len(db.segments)
		if initialSegments < 3 {
			t.Fatalf("expected at least 3 segments before compaction, got %d", initialSegments)
		}

		initialSize, err := db.Size()
		if err != nil {
			t.Fatalf("Size failed: %v", err)
		}

		t.Logf("Before compaction: %d segments, total size %d bytes", initialSegments, initialSize)

		db.Compact()

		done := make(chan struct{})
		go func() {
			db.compactionWg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("Compaction timed out")
		}

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

		if err := db.Close(); err != nil {
			t.Fatalf("failed to close after compaction: %v", err)
		}
		dbReopened, err := Open(tmpDir, 20)
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
		numWorkers := 5
		numOpsPerWorker := 200

		expected := make(map[string]string)
		var expectedMu sync.Mutex

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < numOpsPerWorker; j++ {
					key := fmt.Sprintf("ck-%d-%d", workerID, j)
					value := fmt.Sprintf("cv-%d-%d", workerID, j)

					if err := db.Put(key, value); err != nil {
						t.Errorf("worker %d: Put failed for %s: %v", workerID, key, err)
						return
					}
					expectedMu.Lock()
					expected[key] = value
					expectedMu.Unlock()

					getKey := fmt.Sprintf("ck-%d-%d", (workerID+1)%numWorkers, j)
					_, err := db.Get(getKey)
					if err != nil && !errors.Is(err, ErrNotFound) {
						t.Errorf("worker %d: Get failed for %s: %v", workerID, getKey, err)
						return
					}
				}
			}(i)
		}
		wg.Wait()

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
