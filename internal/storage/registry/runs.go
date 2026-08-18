package registry

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/segment"
)

const registryRunFanIn = 32
const registryRunChunkBytes = 4 * 1024 * 1024
const maxRegistryRunRecordLength = 36 + 2*int(^uint16(0))

func buildRegistryRun(root, buildDir string, descriptors []segment.Descriptor) (string, error) {
	var runs []string
	var chunk []format.RegistryEntry
	chunkBytes := 0
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		slices.SortFunc(chunk, compareEntries)
		path := filepath.Join(buildDir, fmt.Sprintf("run-0-%08d.bin", len(runs)))
		if err := writeRegistryRun(path, chunk); err != nil {
			return err
		}
		runs = append(runs, path)
		chunk = nil
		chunkBytes = 0
		return nil
	}
	err := visitFacts(root, descriptors, func(entry format.RegistryEntry) error {
		entryBytes := 36 + len(entry.Namespace) + len(entry.StreamName)
		if len(chunk) > 0 && chunkBytes > registryRunChunkBytes-entryBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		chunk = append(chunk, entry)
		chunkBytes += entryBytes
		return nil
	})
	if err != nil {
		return "", err
	}
	if err = flush(); err != nil {
		return "", err
	}
	if len(runs) == 0 {
		path := filepath.Join(buildDir, "run-0-00000000.bin")
		file, createErr := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if createErr != nil {
			return "", createErr
		}
		if closeErr := file.Close(); closeErr != nil {
			return "", closeErr
		}
		runs = append(runs, path)
	}
	for round := 1; len(runs) > 1; round++ {
		next := make([]string, 0, (len(runs)+registryRunFanIn-1)/registryRunFanIn)
		for start := 0; start < len(runs); start += registryRunFanIn {
			end := min(start+registryRunFanIn, len(runs))
			output := filepath.Join(buildDir, fmt.Sprintf("run-%d-%08d.bin", round, len(next)))
			if err = mergeRegistryRuns(runs[start:end], output); err != nil {
				return "", err
			}
			for _, input := range runs[start:end] {
				if err = os.Remove(input); err != nil {
					return "", err
				}
			}
			next = append(next, output)
		}
		runs = next
	}
	return runs[0], nil
}

func compareEntries(a, b format.RegistryEntry) int {
	if a.Namespace < b.Namespace {
		return -1
	}
	if a.Namespace > b.Namespace {
		return 1
	}
	if a.StreamName < b.StreamName {
		return -1
	}
	if a.StreamName > b.StreamName {
		return 1
	}
	return 0
}

func writeRegistryRun(path string, entries []format.RegistryEntry) (resultErr error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(file, 256*1024)
	defer func() { resultErr = errors.Join(resultErr, writer.Flush(), file.Close()) }()
	for _, entry := range entries {
		encoded, encodeErr := format.MarshalRegistryEntry(entry)
		if encodeErr != nil {
			return encodeErr
		}
		if _, err = writer.Write(encoded); err != nil {
			return err
		}
	}
	return nil
}

type registryRunReader struct {
	file   *os.File
	reader *bufio.Reader
}

func openRegistryRun(path string) (*registryRunReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &registryRunReader{file: file, reader: bufio.NewReaderSize(file, 64*1024)}, nil
}

func (r *registryRunReader) next() (format.RegistryEntry, bool, error) {
	var prefix [4]byte
	n, err := io.ReadFull(r.reader, prefix[:])
	if errors.Is(err, io.EOF) && n == 0 {
		return format.RegistryEntry{}, false, nil
	}
	if err != nil {
		return format.RegistryEntry{}, false, err
	}
	length := int(binary.LittleEndian.Uint32(prefix[:]))
	if length < 36 || length > maxRegistryRunRecordLength {
		return format.RegistryEntry{}, false, fmt.Errorf("Registry run record length is invalid")
	}
	encoded := make([]byte, length)
	copy(encoded, prefix[:])
	if _, err = io.ReadFull(r.reader, encoded[4:]); err != nil {
		return format.RegistryEntry{}, false, err
	}
	entry, err := format.UnmarshalRegistryEntry(encoded)
	return entry, err == nil, err
}

func (r *registryRunReader) close() error { return r.file.Close() }

type registryHeapItem struct {
	entry format.RegistryEntry
	run   int
}

type registryHeap []registryHeapItem

func (h registryHeap) Len() int           { return len(h) }
func (h registryHeap) Less(i, j int) bool { return compareEntries(h[i].entry, h[j].entry) < 0 }
func (h registryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *registryHeap) Push(value any)    { *h = append(*h, value.(registryHeapItem)) }
func (h *registryHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	*h = old[:len(old)-1]
	return item
}

func mergeRegistryRuns(inputs []string, output string) (resultErr error) {
	readers := make([]*registryRunReader, 0, len(inputs))
	defer func() {
		for _, reader := range readers {
			resultErr = errors.Join(resultErr, reader.close())
		}
	}()
	queue := make(registryHeap, 0, len(inputs))
	for i, input := range inputs {
		reader, err := openRegistryRun(input)
		if err != nil {
			return err
		}
		readers = append(readers, reader)
		entry, ok, err := reader.next()
		if err != nil {
			return err
		}
		if ok {
			heap.Push(&queue, registryHeapItem{entry: entry, run: i})
		}
	}
	file, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(file, 256*1024)
	defer func() { resultErr = errors.Join(resultErr, writer.Flush(), file.Close()) }()
	for queue.Len() > 0 {
		item := heap.Pop(&queue).(registryHeapItem)
		encoded, err := format.MarshalRegistryEntry(item.entry)
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
			heap.Push(&queue, registryHeapItem{entry: next, run: item.run})
		}
	}
	return nil
}

func scanRegistryRun(path string, visit func(format.RegistryEntry) error) (resultErr error) {
	if visit == nil {
		return fmt.Errorf("Registry run visitor is required")
	}
	reader, err := openRegistryRun(path)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, reader.close()) }()
	for {
		entry, ok, err := reader.next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err = visit(entry); err != nil {
			return err
		}
	}
}
