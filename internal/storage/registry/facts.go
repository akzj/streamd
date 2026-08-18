package registry

import (
	"fmt"
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
	var extents []registryExtent
	for _, descriptor := range descriptors {
		err := segment.VisitDirectories(root, descriptor, func(directory format.StreamDirectoryEntry) error {
			if directory.StreamID == RegistryStreamID {
				extents = append(extents, registryExtent{descriptor: descriptor, directory: directory})
			}
			return nil
		})
		if err != nil {
			return nil, err
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
	rebuilt := New()
	nextSequence := uint64(0)
	for _, extent := range extents {
		if extent.directory.FirstSequence != nextSequence {
			return nil, fmt.Errorf("Registry Stream extents are not continuous")
		}
		reader, err := segment.OpenReference(root, extent.descriptor.Reference)
		if err != nil {
			return nil, err
		}
		for sequence := extent.directory.FirstSequence; sequence < extent.directory.FirstSequence+extent.directory.RecordCount; sequence++ {
			record, readErr := reader.Read(RegistryStreamID, sequence)
			if readErr != nil {
				reader.Close()
				return nil, readErr
			}
			registryRecord, decodeErr := format.UnmarshalRegistryRecord(record.Payload)
			if decodeErr != nil || registryRecord.AssignedStreamID != sequence+1 {
				reader.Close()
				return nil, fmt.Errorf("Registry Stream assignment is not contiguous at Sequence %d", sequence)
			}
			if readErr = rebuilt.ApplyRecord(record.EntryID, record.Payload); readErr != nil {
				reader.Close()
				return nil, readErr
			}
		}
		if err = reader.Close(); err != nil {
			return nil, err
		}
		nextSequence += extent.directory.RecordCount
	}
	rebuilt.mu.RLock()
	mappings := make([]Mapping, 0, len(rebuilt.byKey))
	for _, mapping := range rebuilt.byKey {
		mappings = append(mappings, mapping)
	}
	rebuilt.mu.RUnlock()
	return mappings, nil
}
