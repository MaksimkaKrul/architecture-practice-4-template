package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	segmentPrefix = "segment-"
	mergeSuffix   = ".merge"
)

var (
	ErrNotFound = errors.New("record does not exist")
)

type Segment struct {
	num    int
	file   *os.File
	offset int64
}

type SegmentPos struct {
	segmentNum int
	offset     int64
}

type putRequest struct {
	key    string
	value  string
	respCh chan error
}

type getRequest struct {
	key      string
	offset   int64
	filePath string
	respCh   chan getResponse
}

type getResponse struct {
	value string
	err   error
}

type Db struct {
	mu             sync.Mutex
	dir            string
	segments       []*Segment
	index          map[string]SegmentPos
	maxSegmentSize int64

	compactionWg sync.WaitGroup
	compactionMu sync.Mutex
	isCompacting bool

	putRequests chan putRequest
	writerWg    sync.WaitGroup

	getRequests   chan getRequest
	getWorkersWg  sync.WaitGroup
	numGetWorkers int
}

func Open(dir string, maxSegmentSize int64) (*Db, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var segmentFiles []string
	for _, f := range files {
		name := f.Name()
		if strings.HasPrefix(name, segmentPrefix) && !strings.HasSuffix(name, mergeSuffix) {
			segmentFiles = append(segmentFiles, name)
		}
	}

	sort.Slice(segmentFiles, func(i, j int) bool {
		return extractNum(segmentFiles[i]) < extractNum(segmentFiles[j])
	})

	db := &Db{
		dir:            dir,
		segments:       make([]*Segment, 0),
		index:          make(map[string]SegmentPos),
		maxSegmentSize: maxSegmentSize,
		putRequests:    make(chan putRequest, 100),
		numGetWorkers:  runtime.NumCPU() * 2,
		getRequests:    make(chan getRequest),
	}

	for _, segFile := range segmentFiles {
		seg, err := openSegment(dir, segFile)
		if err != nil {
			db.Close()
			return nil, err
		}
		db.segments = append(db.segments, seg)
	}

	if len(db.segments) == 0 {
		seg, err := createNewSegment(dir, 1)
		if err != nil {
			return nil, err
		}
		db.segments = append(db.segments, seg)
	}

	for _, seg := range db.segments {
		if err := db.recoverSegment(seg); err != nil {
			db.Close()
			return nil, err
		}
	}

	db.writerWg.Add(1)
	go db.writerGoroutine()

	for i := 0; i < db.numGetWorkers; i++ {
		db.getWorkersWg.Add(1)
		go db.getWorker()
	}

	return db, nil
}

func extractNum(filename string) int {
	parts := strings.SplitN(filename, "-", 2)
	if len(parts) != 2 {
		return 0
	}
	numStr := strings.TrimLeft(parts[1], "0")
	if numStr == "" {
		return 0
	}
	num, _ := strconv.Atoi(numStr)
	return num
}

func createNewSegment(dir string, num int) (*Segment, error) {
	name := fmt.Sprintf("%s%04d", segmentPrefix, num)
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Segment{
		num:    num,
		file:   f,
		offset: stat.Size(),
	}, nil
}

func openSegment(dir, name string) (*Segment, error) {
	num := extractNum(name)
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Segment{
		num:    num,
		file:   f,
		offset: stat.Size(),
	}, nil
}

func (db *Db) recoverSegment(seg *Segment) error {
	file, err := os.Open(seg.file.Name())
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var offset int64 = 0

	for {
		var record entry
		n, err := record.DecodeFromReader(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("error recovering segment %d at offset %d: %w", seg.num, offset, err)
		}

		db.index[record.key] = SegmentPos{seg.num, offset}
		offset += int64(n)
	}
	return nil
}

func (db *Db) writerGoroutine() {
	defer db.writerWg.Done()
	for req := range db.putRequests {
		db.mu.Lock()
		err := db.performPut(req.key, req.value)
		db.mu.Unlock()
		req.respCh <- err
	}
}

func (db *Db) performPut(key, value string) error {
	activeSeg := db.getActiveSegment()
	if activeSeg.offset >= db.maxSegmentSize {
		newSeg, err := createNewSegment(db.dir, activeSeg.num+1)
		if err != nil {
			return err
		}
		db.segments = append(db.segments, newSeg)
		activeSeg = newSeg
	}

	e := entry{key: key, value: value}
	data := e.Encode()

	n, err := activeSeg.file.Write(data)
	if err != nil {
		return err
	}

	db.index[key] = SegmentPos{activeSeg.num, activeSeg.offset}
	activeSeg.offset += int64(n)
	return nil
}

func (db *Db) Put(key, value string) error {
	req := putRequest{
		key:    key,
		value:  value,
		respCh: make(chan error, 1),
	}

	db.putRequests <- req

	return <-req.respCh
}

func (db *Db) getWorker() {
	defer db.getWorkersWg.Done()
	for req := range db.getRequests {
		value, err := db.readRecordFromFile(req.key, req.offset, req.filePath)
		req.respCh <- getResponse{value: value, err: err}
	}
}

func (db *Db) readRecordFromFile(key string, offset int64, filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open segment file %s for key %s: %w", filePath, key, err)
	}
	defer file.Close()

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return "", fmt.Errorf("failed to seek to offset %d in segment %s for key %s: %w", offset, filePath, key, err)
	}

	var record entry
	_, err = record.DecodeFromReader(bufio.NewReader(file))
	if err != nil {
		return "", fmt.Errorf("failed to decode record at offset %d in segment %s for key %s: %w", offset, filePath, key, err)
	}

	if record.key != key {
		return "", fmt.Errorf("key mismatch: expected %s, got %s at offset %d in segment %s", key, record.key, offset, filePath)
	}

	return record.value, nil
}

func (db *Db) Get(key string) (string, error) {
	db.mu.Lock()
	pos, ok := db.index[key]
	if !ok {
		db.mu.Unlock()
		return "", ErrNotFound
	}

	seg := db.findSegment(pos.segmentNum)
	if seg == nil {
		db.mu.Unlock()
		return "", fmt.Errorf("segment %d for key %s not found in active segments during lookup", pos.segmentNum, key)
	}
	filePath := seg.file.Name()
	db.mu.Unlock()

	req := getRequest{
		key:      key,
		offset:   pos.offset,
		filePath: filePath,
		respCh:   make(chan getResponse, 1),
	}

	db.getRequests <- req

	resp := <-req.respCh
	return resp.value, resp.err
}

func (db *Db) getActiveSegment() *Segment {
	return db.segments[len(db.segments)-1]
}

func (db *Db) findSegment(num int) *Segment {
	for _, seg := range db.segments {
		if seg.num == num {
			return seg
		}
	}
	return nil
}

func (db *Db) Compact() {
	db.compactionMu.Lock()
	if db.isCompacting {
		db.compactionMu.Unlock()
		fmt.Println("Compaction already in progress, skipping new request.")
		return
	}
	db.isCompacting = true
	db.compactionMu.Unlock()

	db.compactionWg.Add(1)
	go func() {
		defer db.compactionWg.Done()
		defer func() {
			db.compactionMu.Lock()
			db.isCompacting = false
			db.compactionMu.Unlock()
		}()

		fmt.Println("Starting background compaction...")
		if err := db.performCompaction(); err != nil {
			fmt.Fprintf(os.Stderr, "Background compaction failed: %v\n", err)
		} else {
			fmt.Println("Background compaction completed successfully.")
		}
	}()
}

func (db *Db) performCompaction() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(db.segments) < 2 {
		return nil
	}

	mergeNum := db.segments[len(db.segments)-1].num + 1
	mergeName := fmt.Sprintf("%s%04d%s", segmentPrefix, mergeNum, mergeSuffix)
	mergePath := filepath.Join(db.dir, mergeName)

	mergeFile, err := os.Create(mergePath)
	if err != nil {
		return err
	}

	mergedKeys := make(map[string]entry)
	segmentsToCompact := db.segments[:len(db.segments)-1]

	for _, seg := range segmentsToCompact {
		if err := processSegmentForCompaction(seg, mergedKeys); err != nil {
			mergeFile.Close()
			os.Remove(mergePath)
			return err
		}
	}

	if err := writeMergedData(mergeFile, mergedKeys); err != nil {
		mergeFile.Close()
		os.Remove(mergePath)
		return err
	}

	if err := mergeFile.Close(); err != nil {
		os.Remove(mergePath)
		return fmt.Errorf("failed to close merge file %s: %w", mergePath, err)
	}

	newSegmentOnePath := filepath.Join(db.dir, fmt.Sprintf("%s%04d", segmentPrefix, 1))
	if err := os.Rename(mergePath, newSegmentOnePath); err != nil {
		fmt.Fprintf(os.Stderr, "Error renaming %s to %s: %v\n", mergePath, newSegmentOnePath, err)
		return err
	}

	currentActiveSeg := db.segments[len(db.segments)-1]
	if err := currentActiveSeg.file.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Error closing current active segment file %s during compaction: %v\n", currentActiveSeg.file.Name(), err)
	}

	oldActiveSegPath := currentActiveSeg.file.Name()
	newActiveSegNum := 2
	newActiveSegPath := filepath.Join(db.dir, fmt.Sprintf("%s%04d", segmentPrefix, newActiveSegNum))

	if oldActiveSegPath != newActiveSegPath {
		if err := os.Rename(oldActiveSegPath, newActiveSegPath); err != nil {
			fmt.Fprintf(os.Stderr, "Error renaming active segment %s to %s: %v\n", oldActiveSegPath, newActiveSegPath, err)
			return err
		}
	}

	db.segments = make([]*Segment, 0)
	db.index = make(map[string]SegmentPos)

	files, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}

	var postCompactSegmentFiles []string
	for _, f := range files {
		name := f.Name()
		if strings.HasPrefix(name, segmentPrefix) && !strings.HasSuffix(name, mergeSuffix) {
			postCompactSegmentFiles = append(postCompactSegmentFiles, name)
		}
	}

	sort.Slice(postCompactSegmentFiles, func(i, j int) bool {
		return extractNum(postCompactSegmentFiles[i]) < extractNum(postCompactSegmentFiles[j])
	})

	for _, segFile := range postCompactSegmentFiles {
		seg, err := openSegment(db.dir, segFile)
		if err != nil {
			return fmt.Errorf("failed to open segment %s after compaction: %w", segFile, err)
		}
		db.segments = append(db.segments, seg)
		if err := db.recoverSegment(seg); err != nil {
			return fmt.Errorf("failed to recover segment %s after compaction: %w", segFile, err)
		}
	}

	if len(db.segments) == 0 {
		seg, err := createNewSegment(db.dir, 1)
		if err != nil {
			return err
		}
		db.segments = append(db.segments, seg)
	}

	return nil
}

func processSegmentForCompaction(seg *Segment, mergedKeys map[string]entry) error {
	file, err := os.Open(seg.file.Name())
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		var record entry
		_, err := record.DecodeFromReader(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		mergedKeys[record.key] = record
	}
	return nil
}

func writeMergedData(file *os.File, data map[string]entry) error {
	writer := bufio.NewWriter(file)
	defer writer.Flush()

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		record := data[k]
		if _, err := writer.Write(record.Encode()); err != nil {
			return err
		}
	}
	return nil
}

func (db *Db) Close() error {
	close(db.putRequests)
	db.writerWg.Wait()

	close(db.getRequests)
	db.getWorkersWg.Wait()

	db.compactionWg.Wait()

	db.mu.Lock()
	defer db.mu.Unlock()

	var errs []error
	for _, seg := range db.segments {
		if seg.file != nil {
			if err := seg.file.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close segment file %s: %w", seg.file.Name(), err))
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing segments: %v", errs)
	}
	return nil
}

func (db *Db) Size() (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	var total int64
	for _, seg := range db.segments {
		stat, err := seg.file.Stat()
		if err != nil {
			return 0, err
		}
		total += stat.Size()
	}
	return total, nil
}
