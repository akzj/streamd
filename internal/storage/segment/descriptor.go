package segment

import (
	"fmt"
	"path/filepath"

	"github.com/akzj/streamd/internal/storage/format"
)

// Descriptor is the immutable metadata needed to locate records without
// keeping the Segment file open.
type Descriptor struct {
	Reference   format.SegmentReference
	Header      format.SegmentHeader
	Directories []format.StreamDirectoryEntry
	Footer      format.SegmentFooter
}

// OpenReference opens a Segment and verifies that its inexpensive metadata
// matches the current Manifest reference. Full content hashing remains the
// responsibility of scrub and publication validation.
func OpenReference(root string, reference format.SegmentReference) (*Reader, error) {
	if reference.Flags&format.SegmentRefHasLocal == 0 {
		return nil, fmt.Errorf("Segment %x has no local copy", reference.SegmentID)
	}
	reader, err := Open(filepath.Join(root, reference.LocalPath))
	if err != nil {
		return nil, err
	}
	if err = reader.validateReference(reference); err != nil {
		reader.Close()
		return nil, err
	}
	return reader, nil
}

// DescribeReference validates a Segment and returns detached metadata. The
// returned value does not retain an open file descriptor.
func DescribeReference(root string, reference format.SegmentReference) (Descriptor, error) {
	reader, err := OpenReference(root, reference)
	if err != nil {
		return Descriptor{}, err
	}
	descriptor := Descriptor{
		Reference:   reference,
		Header:      reader.Header,
		Directories: append([]format.StreamDirectoryEntry(nil), reader.Directories...),
		Footer:      reader.Footer,
	}
	if err = reader.Close(); err != nil {
		return Descriptor{}, err
	}
	return descriptor, nil
}

func (r *Reader) validateReference(reference format.SegmentReference) error {
	if r.Header.SegmentID != reference.SegmentID ||
		r.Footer.FileLength != reference.FileSize ||
		r.Header.FirstEntryID != reference.FirstEntryID ||
		r.Header.LastEntryID != reference.LastEntryID ||
		r.Header.StreamCount != reference.StreamCount ||
		r.Header.RecordCount != reference.RecordCount {
		return fmt.Errorf("Segment %x does not match Manifest Reference", reference.SegmentID)
	}
	return nil
}
