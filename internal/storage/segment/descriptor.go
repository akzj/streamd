package segment

import (
	"fmt"
	"io"
	"os"
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

// DescribeReferenceLight validates the fixed Segment metadata referenced by a
// Manifest without reading the Stream Directory. It is the recovery path for
// keeping startup memory and I/O independent of the number of historical
// Extents. Directory entries remain available through OpenReference when a
// projection must be rebuilt or a targeted fallback lookup is required.
func DescribeReferenceLight(root string, reference format.SegmentReference) (Descriptor, error) {
	if reference.Flags&format.SegmentRefHasLocal == 0 {
		return Descriptor{}, fmt.Errorf("Segment %x has no local copy", reference.SegmentID)
	}
	file, err := os.Open(filepath.Join(root, reference.LocalPath))
	if err != nil {
		return Descriptor{}, err
	}
	defer file.Close()
	headerSection := make([]byte, format.SegmentSectionAlignment)
	if _, err = io.ReadFull(io.NewSectionReader(file, 0, int64(len(headerSection))), headerSection); err != nil {
		return Descriptor{}, err
	}
	header, err := format.UnmarshalSegmentHeaderSection(headerSection)
	if err != nil {
		return Descriptor{}, err
	}
	info, err := file.Stat()
	if err != nil {
		return Descriptor{}, err
	}
	if uint64(info.Size()) != header.FooterOffset+format.SegmentFooterSectionLength {
		return Descriptor{}, fmt.Errorf("Segment file length mismatch")
	}
	footerSection := make([]byte, format.SegmentFooterSectionLength)
	if _, err = file.ReadAt(footerSection, int64(header.FooterOffset)); err != nil {
		return Descriptor{}, err
	}
	footer, err := format.UnmarshalSegmentFooter(footerSection[:format.SegmentFooterLength])
	if err != nil {
		return Descriptor{}, err
	}
	for _, value := range footerSection[format.SegmentFooterLength:] {
		if value != 0 {
			return Descriptor{}, fmt.Errorf("Segment Footer padding is not zero")
		}
	}
	if footer.SegmentID != header.SegmentID || footer.FileLength != uint64(info.Size()) || footer.ContentLength != header.FooterOffset || footer.RecordCount != header.RecordCount || footer.StreamCount != header.StreamCount {
		return Descriptor{}, fmt.Errorf("Segment Footer does not match Header")
	}
	descriptor := Descriptor{Reference: reference, Header: header, Footer: footer}
	if header.SegmentID != reference.SegmentID || footer.FileLength != reference.FileSize || footer.ContentSHA256 != reference.ContentSHA256 || header.FirstEntryID != reference.FirstEntryID || header.LastEntryID != reference.LastEntryID || header.StreamCount != reference.StreamCount || header.RecordCount != reference.RecordCount {
		return Descriptor{}, fmt.Errorf("Segment %x does not match Manifest Reference", reference.SegmentID)
	}
	return descriptor, nil
}

// MaterializeDescriptors loads and validates Stream Directories for an
// explicit maintenance operation. Runtime recovery deliberately keeps only
// light descriptors.
func MaterializeDescriptors(root string, descriptors []Descriptor) ([]Descriptor, error) {
	result := make([]Descriptor, 0, len(descriptors))
	for _, descriptor := range descriptors {
		if descriptor.Directories != nil {
			result = append(result, descriptor)
			continue
		}
		loaded, err := DescribeReference(root, descriptor.Reference)
		if err != nil {
			return nil, err
		}
		result = append(result, loaded)
	}
	return result, nil
}

func LightDescriptors(descriptors []Descriptor) []Descriptor {
	result := make([]Descriptor, len(descriptors))
	for i, descriptor := range descriptors {
		descriptor.Directories = nil
		result[i] = descriptor
	}
	return result
}

// LatestRecordedAt reads only the newest checkpoint Segment Directory to
// recover the global timestamp clamp. Segment size bounds this work; recovery
// does not inspect directories from the historical Segment set.
func LatestRecordedAt(root string, descriptors []Descriptor) (int64, error) {
	if len(descriptors) == 0 {
		return 0, nil
	}
	latest := descriptors[0]
	for _, descriptor := range descriptors[1:] {
		if descriptor.Reference.LastEntryID > latest.Reference.LastEntryID {
			latest = descriptor
		}
	}
	reader, err := OpenReference(root, latest.Reference)
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	for _, directory := range reader.Directories {
		if directory.LastEntryID == latest.Reference.LastEntryID {
			return directory.LastRecordedAt, nil
		}
	}
	return 0, fmt.Errorf("latest Segment Entry %d has no Stream Directory", latest.Reference.LastEntryID)
}

func (r *Reader) validateReference(reference format.SegmentReference) error {
	if r.Header.SegmentID != reference.SegmentID ||
		r.Footer.FileLength != reference.FileSize ||
		r.Footer.ContentSHA256 != reference.ContentSHA256 ||
		r.Header.FirstEntryID != reference.FirstEntryID ||
		r.Header.LastEntryID != reference.LastEntryID ||
		r.Header.StreamCount != reference.StreamCount ||
		r.Header.RecordCount != reference.RecordCount {
		return fmt.Errorf("Segment %x does not match Manifest Reference", reference.SegmentID)
	}
	return nil
}
