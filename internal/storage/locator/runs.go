package locator

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/segment"
)

const extentRunFanIn = 32
const extentRunRecordLength = 16 + 10*8

type extentRecord struct {
	segmentID format.UUID
	directory format.StreamDirectoryEntry
}

func buildExtentRun(root, buildDir string, descriptors []segment.Descriptor) (string, error) {
	runs := make([]string, 0, len(descriptors))
	for i, descriptor := range descriptors {
		path := filepath.Join(buildDir, fmt.Sprintf("run-0-%08d.bin", i))
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return "", err
		}
		writer := bufio.NewWriterSize(file, 128*1024)
		var previous *format.StreamDirectoryEntry
		err = segment.VisitDirectories(root, descriptor, func(directory format.StreamDirectoryEntry) error {
			if previous != nil && (previous.StreamID > directory.StreamID || previous.StreamID == directory.StreamID && previous.FirstSequence >= directory.FirstSequence) {
				return fmt.Errorf("Segment %x Directory is not strictly ordered", descriptor.Reference.SegmentID)
			}
			encoded, encodeErr := marshalExtentRecord(extentRecord{segmentID: descriptor.Reference.SegmentID, directory: directory})
			if encodeErr == nil {
				_, encodeErr = writer.Write(encoded)
			}
			copy := directory
			previous = &copy
			return encodeErr
		})
		closeErr := errors.Join(writer.Flush(), file.Close())
		if err != nil || closeErr != nil {
			return "", errors.Join(err, closeErr)
		}
		runs = append(runs, path)
	}
	if len(runs) == 0 {
		return "", fmt.Errorf("Locator build has no Segment runs")
	}
	for round := 1; len(runs) > 1; round++ {
		next := make([]string, 0, (len(runs)+extentRunFanIn-1)/extentRunFanIn)
		for start := 0; start < len(runs); start += extentRunFanIn {
			end := min(start+extentRunFanIn, len(runs))
			output := filepath.Join(buildDir, fmt.Sprintf("run-%d-%08d.bin", round, len(next)))
			if err := mergeExtentRuns(runs[start:end], output); err != nil {
				return "", err
			}
			for _, input := range runs[start:end] {
				if err := os.Remove(input); err != nil {
					return "", err
				}
			}
			next = append(next, output)
		}
		runs = next
	}
	return runs[0], nil
}

func marshalExtentRecord(record extentRecord) ([]byte, error) {
	if record.segmentID == (format.UUID{}) {
		return nil, fmt.Errorf("Locator extent Segment ID is zero")
	}
	encoded := make([]byte, extentRunRecordLength)
	copy(encoded[:16], record.segmentID[:])
	values := []uint64{
		record.directory.StreamID, record.directory.FirstSequence, record.directory.RecordCount,
		record.directory.FirstByteOffset, record.directory.NextByteOffset,
		uint64(record.directory.FirstRecordedAt), uint64(record.directory.LastRecordedAt),
		record.directory.LastEntryID, record.directory.RecordIndexOffset, record.directory.StreamDataOffset,
	}
	for i, value := range values {
		binary.LittleEndian.PutUint64(encoded[16+i*8:24+i*8], value)
	}
	return encoded, nil
}

func unmarshalExtentRecord(encoded []byte) (extentRecord, error) {
	var record extentRecord
	if len(encoded) != extentRunRecordLength {
		return record, fmt.Errorf("Locator extent run record length is invalid")
	}
	copy(record.segmentID[:], encoded[:16])
	if record.segmentID == (format.UUID{}) {
		return extentRecord{}, fmt.Errorf("Locator extent Segment ID is zero")
	}
	value := func(i int) uint64 { return binary.LittleEndian.Uint64(encoded[16+i*8 : 24+i*8]) }
	record.directory = format.StreamDirectoryEntry{
		StreamID: value(0), FirstSequence: value(1), RecordCount: value(2),
		FirstByteOffset: value(3), NextByteOffset: value(4),
		FirstRecordedAt: int64(value(5)), LastRecordedAt: int64(value(6)),
		LastEntryID: value(7), RecordIndexOffset: value(8), StreamDataOffset: value(9),
	}
	return record, nil
}

type extentRunReader struct {
	file   *os.File
	reader *bufio.Reader
}

func openExtentRun(path string) (*extentRunReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &extentRunReader{file: file, reader: bufio.NewReaderSize(file, 64*1024)}, nil
}

func (r *extentRunReader) next() (extentRecord, bool, error) {
	encoded := make([]byte, extentRunRecordLength)
	n, err := io.ReadFull(r.reader, encoded)
	if errors.Is(err, io.EOF) && n == 0 {
		return extentRecord{}, false, nil
	}
	if err != nil {
		return extentRecord{}, false, err
	}
	record, err := unmarshalExtentRecord(encoded)
	return record, err == nil, err
}

func (r *extentRunReader) close() error { return r.file.Close() }

type extentHeapItem struct {
	record extentRecord
	run    int
}

type extentHeap []extentHeapItem

func (h extentHeap) Len() int { return len(h) }
func (h extentHeap) Less(i, j int) bool {
	a, b := h[i].record, h[j].record
	if a.directory.StreamID != b.directory.StreamID {
		return a.directory.StreamID < b.directory.StreamID
	}
	if a.directory.FirstSequence != b.directory.FirstSequence {
		return a.directory.FirstSequence < b.directory.FirstSequence
	}
	for k := range a.segmentID {
		if a.segmentID[k] != b.segmentID[k] {
			return a.segmentID[k] < b.segmentID[k]
		}
	}
	return false
}
func (h extentHeap) Swap(i, j int)   { h[i], h[j] = h[j], h[i] }
func (h *extentHeap) Push(value any) { *h = append(*h, value.(extentHeapItem)) }
func (h *extentHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	*h = old[:len(old)-1]
	return item
}

func mergeExtentRuns(inputs []string, output string) (resultErr error) {
	readers := make([]*extentRunReader, 0, len(inputs))
	defer func() {
		for _, reader := range readers {
			resultErr = errors.Join(resultErr, reader.close())
		}
	}()
	queue := make(extentHeap, 0, len(inputs))
	for i, input := range inputs {
		reader, err := openExtentRun(input)
		if err != nil {
			return err
		}
		readers = append(readers, reader)
		record, ok, err := reader.next()
		if err != nil {
			return err
		}
		if ok {
			heap.Push(&queue, extentHeapItem{record: record, run: i})
		}
	}
	file, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(file, 256*1024)
	defer func() { resultErr = errors.Join(resultErr, writer.Flush(), file.Close()) }()
	for queue.Len() > 0 {
		item := heap.Pop(&queue).(extentHeapItem)
		encoded, err := marshalExtentRecord(item.record)
		if err != nil {
			return err
		}
		if _, err = writer.Write(encoded); err != nil {
			return err
		}
		next, ok, err := readers[item.run].next()
		if err != nil {
			return err
		}
		if ok {
			heap.Push(&queue, extentHeapItem{record: next, run: item.run})
		}
	}
	return nil
}

func scanExtentRun(path string, visit func(extentRecord) error) (resultErr error) {
	reader, err := openExtentRun(path)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, reader.close()) }()
	for {
		record, ok, err := reader.next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err = visit(record); err != nil {
			return err
		}
	}
}
