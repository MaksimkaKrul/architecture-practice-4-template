package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

type Db struct {
	mu             sync.Mutex
	dir            string
	segments       []*Segment
	index          map[string]SegmentPos
	maxSegmentSize int64

	// New fields for compaction management
	compactionWg sync.WaitGroup // To wait for active compactions on close
	compactionMu sync.Mutex     // To ensure only one compaction runs at a time
	isCompacting bool           // Flag to prevent multiple compactions
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
		// Пропускаем временные файлы с суффиксом merge
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
	}

	for _, segFile := range segmentFiles {
		seg, err := openSegment(dir, segFile)
		if err != nil {
			// В случае ошибки при открытии сегмента, попробуем закрыть уже открытые
			db.Close() // Закрываем все, что успели открыть
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

	// Восстановление индекса для всех сегментов
	for _, seg := range db.segments {
		if err := db.recoverSegment(seg); err != nil {
			db.Close() // Закрываем все при ошибке восстановления
			return nil, err
		}
	}

	return db, nil
}

func extractNum(filename string) int {
	parts := strings.SplitN(filename, "-", 2)
	if len(parts) != 2 {
		return 0
	}
	numStr := strings.TrimLeft(parts[1], "0")
	if numStr == "" { // Handle cases like "segment-0000" or just "segment-"
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
		f.Close() // Закрыть файл при ошибке stat
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
		f.Close() // Закрыть файл при ошибке stat
		return nil, err
	}
	return &Segment{
		num:    num,
		file:   f,
		offset: stat.Size(),
	}, nil
}

func (db *Db) recoverSegment(seg *Segment) error {
	// Для восстановления мы открываем файл для чтения, чтобы не мешать основному файлу,
	// который может быть открыт для записи.
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

func (db *Db) Put(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

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

func (db *Db) Get(key string) (string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	pos, ok := db.index[key]
	if !ok {
		return "", ErrNotFound
	}

	seg := db.findSegment(pos.segmentNum)
	if seg == nil {
		return "", fmt.Errorf("segment %d for key %s not found in active segments", pos.segmentNum, key)
	}

	file, err := os.Open(seg.file.Name())
	if err != nil {
		return "", fmt.Errorf("failed to open segment file %s for key %s: %w", seg.file.Name(), key, err)
	}
	defer file.Close()

	_, err = file.Seek(pos.offset, io.SeekStart)
	if err != nil {
		return "", fmt.Errorf("failed to seek to offset %d in segment %s for key %s: %w", pos.offset, seg.file.Name(), key, err)
	}

	var record entry
	_, err = record.DecodeFromReader(bufio.NewReader(file))
	if err != nil {
		return "", fmt.Errorf("failed to decode record at offset %d in segment %s for key %s: %w", pos.offset, seg.file.Name(), key, err)
	}

	return record.value, nil
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

// Compact запускает операцию компактирования в фоновой горутине.
// Если компакция уже идет, новая не запускается.
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
		defer func() { // Reset flag when done
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

// performCompaction содержит основную логику компактирования.
// Эта функция выполняется под глобальной блокировкой db.mu, что означает,
// что операции Put/Get будут заблокированы на время ее выполнения.
// Для полностью неблокирующей компакции потребовалось бы более сложное управление
// состоянием (например, Copy-on-Write).
func (db *Db) performCompaction() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Нет смысла компактировать, если меньше 2 сегментов (т.е. 0 или 1)
	if len(db.segments) < 2 {
		return nil
	}

	// 1. Создаем временный файл для объединенных данных
	// Номер для merge файла будет на 1 больше номера последнего сегмента
	// (или номер самого последнего сегмента, если он активный)
	// Для компакции, мы всегда начинаем с 1-го сегмента, поэтому имя будет уникальным.
	mergeNum := db.segments[len(db.segments)-1].num + 1 // Номер для временного файла
	mergeName := fmt.Sprintf("%s%04d%s", segmentPrefix, mergeNum, mergeSuffix)
	mergePath := filepath.Join(db.dir, mergeName)

	mergeFile, err := os.Create(mergePath)
	if err != nil {
		return err
	}

	// 2. Собираем актуальные записи из всех сегментов, кроме текущего активного
	// (который не участвует в компактировании)
	mergedKeys := make(map[string]entry)
	// Итерируем по копии segments, чтобы избежать проблем, если db.segments изменяется
	// во время итерации (хотя в текущем performCompaction этого не происходит)
	segmentsToCompact := make([]*Segment, len(db.segments)-1)
	copy(segmentsToCompact, db.segments[:len(db.segments)-1])

	for _, seg := range segmentsToCompact {
		if err := processSegmentForCompaction(seg, mergedKeys, db.index); err != nil {
			mergeFile.Close()    // Закрыть mergeFile при ошибке
			os.Remove(mergePath) // Удалить неполный merge-файл
			return err
		}
	}

	// 3. Записываем объединенные данные во временный файл
	if err := writeMergedData(mergeFile, mergedKeys); err != nil {
		mergeFile.Close()
		os.Remove(mergePath)
		return err
	}

	// 4. Закрываем merge-файл. Это критично для os.Rename на Windows.
	if err := mergeFile.Close(); err != nil {
		os.Remove(mergePath) // Попытаться удалить, если закрытие не удалось
		return fmt.Errorf("failed to close merge file %s: %w", mergePath, err)
	}

	// 5. Закрываем и удаляем старые файлы сегментов
	// Перед удалением файла, его дескриптор должен быть закрыт.
	for _, seg := range segmentsToCompact {
		if err := seg.file.Close(); err != nil {
			// Логируем ошибку, но продолжаем, так как другие файлы могут быть удалены
			fmt.Fprintf(os.Stderr, "Error closing old segment file %s: %v\n", seg.file.Name(), err)
		}
		if err := os.Remove(seg.file.Name()); err != nil {
			fmt.Fprintf(os.Stderr, "Error removing old segment file %s: %v\n", seg.file.Name(), err)
		}
	}

	// 6. Переименовываем объединенный файл в "segment-0001"
	// Это станет новым "первым" сегментом
	newSegmentOnePath := filepath.Join(db.dir, fmt.Sprintf("%s%04d", segmentPrefix, 1))
	if err := os.Rename(mergePath, newSegmentOnePath); err != nil {
		// Если переименование не удалось, возможно, надо откатиться
		fmt.Fprintf(os.Stderr, "Error renaming %s to %s: %v\n", mergePath, newSegmentOnePath, err)
		return err
	}

	// 7. Перестроение структуры Db: очищаем сегменты и индекс, затем переоткрываем их
	// и восстанавливаем индекс из файлов на диске.
	// Закрываем активный сегмент, так как он будет переоткрыт в рамках общей логики.
	activeSeg := db.getActiveSegment() // Получаем ссылку на активный сегмент до очистки
	if activeSeg != nil {
		if err := activeSeg.file.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Error closing active segment file %s during compaction setup: %v\n", activeSeg.file.Name(), err)
		}
		// Если активный сегмент не был частью компактируемых (т.е., не seg-0001),
		// его нужно переименовать, чтобы он шел после нового сегмента 0001.
		// Его новый номер будет 2.
		oldActiveSegPath := activeSeg.file.Name()
		newActiveSegNum := 2 // После компакции seg-0001 будет новым объединенным, старый активный станет seg-0002
		newActiveSegPath := filepath.Join(db.dir, fmt.Sprintf("%s%04d", segmentPrefix, newActiveSegNum))

		if oldActiveSegPath != newActiveSegPath { // Избегаем переименования на то же имя
			if err := os.Rename(oldActiveSegPath, newActiveSegPath); err != nil {
				fmt.Fprintf(os.Stderr, "Error renaming active segment %s to %s: %v\n", oldActiveSegPath, newActiveSegPath, err)
				return err
			}
		}
	}

	// Полностью очищаем текущие сегменты и индекс
	db.segments = make([]*Segment, 0)
	db.index = make(map[string]SegmentPos)

	// Сканируем директорию заново, чтобы найти все актуальные сегменты (новый seg-0001
	// и бывший активный сегмент, который теперь seg-0002)
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

	// Если после компактирования не осталось ни одного сегмента (что маловероятно, но возможно),
	// создаем новый начальный сегмент.
	if len(db.segments) == 0 {
		seg, err := createNewSegment(db.dir, 1)
		if err != nil {
			return err
		}
		db.segments = append(db.segments, seg)
	}

	return nil
}

func processSegmentForCompaction(seg *Segment, mergedKeys map[string]entry, index map[string]SegmentPos) error {
	file, err := os.Open(seg.file.Name()) // Открываем для чтения
	if err != nil {
		return err
	}
	defer file.Close() // Закрываем после чтения

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
	defer writer.Flush() // Важно: Flush() перед закрытием файла

	for _, record := range data {
		if _, err := writer.Write(record.Encode()); err != nil {
			return err
		}
	}
	return nil
}

func (db *Db) Close() error {
	// Ждем завершения любых активных фоновых компакций
	// Это должно быть ДО блокировки db.mu, чтобы горутина компакции могла
	// завершить свою работу и вызвать Done()
	db.compactionWg.Wait()

	db.mu.Lock()
	defer db.mu.Unlock()

	var errs []error
	for _, seg := range db.segments {
		if seg.file != nil { // Проверяем, что файл открыт
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
