package registry

import (
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/segment"
)

type registryExtent struct {
	descriptor segment.Descriptor
	directory  format.StreamDirectoryEntry
}

// RebuildMappings reads the authoritative Registry Stream from immutable
// Segments. It is the correctness fallback for a missing or corrupt Snapshot.
func RebuildMappings(root string, descriptors []segment.Descriptor) ([]Mapping, error) {
	rebuilt := New()
	err := visitFacts(root, descriptors, func(entry format.RegistryEntry) error {
		return rebuilt.ApplyMapping(Mapping{Key: Key{Namespace: entry.Namespace, StreamName: entry.StreamName}, StreamID: entry.StreamID, CreatedEntryID: entry.CreatedEntryID})
	})
	if err != nil {
		return nil, err
	}
	rebuilt.mu.RLock()
	mappings := make([]Mapping, 0, len(rebuilt.byKey))
	for _, mapping := range rebuilt.byKey {
		mappings = append(mappings, mapping)
	}
	rebuilt.mu.RUnlock()
	return mappings, nil
}

func visitFacts(root string, descriptors []segment.Descriptor, visit func(format.RegistryEntry) error) error {
	if visit == nil {
		return fmt.Errorf("Registry fact visitor is required")
	}
	var extents []registryExtent
	for _, descriptor := range descriptors {
		err := segment.VisitDirectories(root, descriptor, func(directory format.StreamDirectoryEntry) error {
			if directory.StreamID == RegistryStreamID {
				extents = append(extents, registryExtent{descriptor: descriptor, directory: directory})
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	slices.SortFunc(extents, func(a, b registryExtent) int {
		if a.directory.FirstSequence < b.directory.FirstSequence {
			return -1
		}
		if a.directory.FirstSequence > b.directory.FirstSequence {
			return 1
		}
		return 0
	})
	nextSequence := uint64(0)
	for _, extent := range extents {
		if extent.directory.FirstSequence != nextSequence {
			return fmt.Errorf("Registry Stream extents are not continuous")
		}
		if extent.directory.RecordCount > math.MaxUint64-extent.directory.FirstSequence {
			return fmt.Errorf("Registry Stream extent Sequence overflows")
		}
		reader, err := segment.OpenReference(root, extent.descriptor.Reference)
		if err != nil {
			return err
		}
		var readErr error
		for i := uint64(0); i < extent.directory.RecordCount; i++ {
			sequence := extent.directory.FirstSequence + i
			record, readErr := reader.Read(RegistryStreamID, sequence)
			if readErr != nil {
				break
			}
			registryRecord, decodeErr := format.UnmarshalRegistryRecord(record.Payload)
			if decodeErr != nil || sequence == math.MaxUint64 || registryRecord.AssignedStreamID != sequence+1 {
				readErr = fmt.Errorf("Registry Stream assignment is not contiguous at Sequence %d", sequence)
				break
			}
			readErr = visit(format.RegistryEntry{StreamID: registryRecord.AssignedStreamID, CreatedEntryID: record.EntryID, Namespace: registryRecord.Namespace, StreamName: registryRecord.StreamName})
			if readErr != nil {
				break
			}
		}
		if err = errors.Join(readErr, reader.Close()); err != nil {
			return err
		}
		nextSequence += extent.directory.RecordCount
	}
	return nil
}
