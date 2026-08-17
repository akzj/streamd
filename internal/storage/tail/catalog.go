package tail

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
)

type Catalog struct {
	file      *os.File
	reference format.ArtifactReference
	header    format.TailCatalogHeader
}

func WriteNewCheckpoint(root string, manifestGeneration, coveredEntryID uint64, slots []format.TailSlot) (format.ArtifactReference, error) {
	var artifactID format.UUID
	if _, err := rand.Read(artifactID[:]); err != nil {
		return format.ArtifactReference{}, err
	}
	return WriteCheckpoint(root, artifactID, manifestGeneration, coveredEntryID, slots)
}

// WriteCheckpoint writes an immutable Tail Catalog without constructing the
// full fixed-slot file in memory. Missing Stream IDs are emitted as zero slots.
func WriteCheckpoint(root string, artifactID format.UUID, manifestGeneration, coveredEntryID uint64, slots []format.TailSlot) (format.ArtifactReference, error) {
	byStream := make(map[uint64][]byte, len(slots))
	var slotCount uint64
	for _, slot := range slots {
		if !slot.Present {
			return format.ArtifactReference{}, fmt.Errorf("Tail checkpoint contains an absent Slot")
		}
		if slot.AppliedEntryID != coveredEntryID {
			return format.ArtifactReference{}, fmt.Errorf("Tail Slot %d applied Entry ID does not match checkpoint", slot.StreamID)
		}
		encoded, err := format.MarshalTailSlot(slot)
		if err != nil {
			return format.ArtifactReference{}, err
		}
		if _, duplicate := byStream[slot.StreamID]; duplicate {
			return format.ArtifactReference{}, fmt.Errorf("duplicate Tail Slot %d", slot.StreamID)
		}
		byStream[slot.StreamID] = encoded
		if slot.StreamID == ^uint64(0) {
			return format.ArtifactReference{}, fmt.Errorf("Tail Slot count overflows")
		}
		if slot.StreamID+1 > slotCount {
			slotCount = slot.StreamID + 1
		}
	}
	header := format.TailCatalogHeader{ArtifactID: artifactID, SlotCount: slotCount, CoveredEntryID: coveredEntryID, ManifestGeneration: manifestGeneration}
	headerBytes, err := format.MarshalTailCatalogHeader(header)
	if err != nil {
		return format.ArtifactReference{}, err
	}
	contentLength, err := format.TailSlotPosition(slotCount)
	if err != nil {
		return format.ArtifactReference{}, err
	}
	if contentLength > math.MaxUint64-format.ArtifactFooterLength {
		return format.ArtifactReference{}, fmt.Errorf("Tail Catalog file length overflows")
	}
	catalogDir := filepath.Join(root, "catalog")
	if err = os.MkdirAll(catalogDir, 0750); err != nil {
		return format.ArtifactReference{}, err
	}
	name := fmt.Sprintf("TAIL-%x.cat", artifactID)
	staging := filepath.Join(catalogDir, "."+name+".tmp")
	final := filepath.Join(catalogDir, name)
	file, err := os.OpenFile(staging, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if err != nil {
		return format.ArtifactReference{}, err
	}
	keep := false
	defer func() {
		_ = file.Close()
		if !keep {
			_ = os.Remove(staging)
		}
	}()
	digest := sha256.New()
	writer := bufio.NewWriterSize(io.MultiWriter(file, digest), 128*1024)
	if _, err = writer.Write(headerBytes); err != nil {
		return format.ArtifactReference{}, err
	}
	zeroSlot, _ := format.MarshalTailSlot(format.TailSlot{})
	slotBase, _ := format.TailSlotPosition(0)
	padding := make([]byte, int(slotBase)-len(headerBytes))
	if _, err = writer.Write(padding); err != nil {
		return format.ArtifactReference{}, err
	}
	for streamID := uint64(0); streamID < slotCount; streamID++ {
		encoded := byStream[streamID]
		if encoded == nil {
			encoded = zeroSlot
		}
		if _, err = writer.Write(encoded); err != nil {
			return format.ArtifactReference{}, err
		}
	}
	if err = writer.Flush(); err != nil {
		return format.ArtifactReference{}, err
	}
	sum := digest.Sum(nil)
	var contentSHA [sha256.Size]byte
	copy(contentSHA[:], sum)
	footer := format.ArtifactFooter{ArtifactType: format.ArtifactTailCatalog, ArtifactID: artifactID, FileLength: contentLength + format.ArtifactFooterLength, ContentLength: contentLength, ContentSHA256: contentSHA}
	footerBytes, err := format.MarshalArtifactFooter(footer)
	if err != nil {
		return format.ArtifactReference{}, err
	}
	if _, err = file.Write(footerBytes); err != nil {
		return format.ArtifactReference{}, err
	}
	if err = file.Sync(); err != nil {
		return format.ArtifactReference{}, err
	}
	if err = file.Close(); err != nil {
		return format.ArtifactReference{}, err
	}
	if err = os.Rename(staging, final); err != nil {
		return format.ArtifactReference{}, err
	}
	keep = true
	if err = fsutil.SyncDir(catalogDir); err != nil {
		return format.ArtifactReference{}, err
	}
	return format.ArtifactReference{ArtifactType: format.ArtifactTailCatalog, FormatVersion: format.VersionV1, ArtifactID: artifactID, FileSize: footer.FileLength, CoveredEntryID: coveredEntryID, Path: filepath.ToSlash(filepath.Join("catalog", name)), ContentSHA256: contentSHA}, nil
}

func OpenCheckpoint(root string, reference format.ArtifactReference, manifestGeneration, coveredEntryID uint64) (*Catalog, error) {
	if reference.ArtifactType != format.ArtifactTailCatalog || reference.FormatVersion != format.VersionV1 || reference.CoveredEntryID != coveredEntryID {
		return nil, fmt.Errorf("Tail Catalog reference does not match Manifest")
	}
	file, err := os.Open(filepath.Join(root, filepath.FromSlash(reference.Path)))
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Catalog, error) {
		file.Close()
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return fail(err)
	}
	if uint64(info.Size()) != reference.FileSize || info.Size() < format.TailCatalogHeaderLength+format.ArtifactFooterLength {
		return fail(fmt.Errorf("Tail Catalog file size does not match Manifest"))
	}
	headerBytes := make([]byte, format.TailCatalogHeaderLength)
	if _, err = file.ReadAt(headerBytes, 0); err != nil {
		return fail(err)
	}
	header, err := format.UnmarshalTailCatalogHeader(headerBytes)
	if err != nil {
		return fail(err)
	}
	if header.ArtifactID != reference.ArtifactID || header.ManifestGeneration != manifestGeneration || header.CoveredEntryID != coveredEntryID {
		return fail(fmt.Errorf("Tail Catalog header does not match Manifest"))
	}
	contentLength, err := format.TailSlotPosition(header.SlotCount)
	if err != nil || contentLength+format.ArtifactFooterLength != reference.FileSize {
		return fail(fmt.Errorf("Tail Catalog Slot count does not match file size"))
	}
	footerBytes := make([]byte, format.ArtifactFooterLength)
	if _, err = file.ReadAt(footerBytes, int64(contentLength)); err != nil {
		return fail(err)
	}
	footer, err := format.UnmarshalArtifactFooter(footerBytes)
	if err != nil {
		return fail(err)
	}
	if footer.ArtifactType != format.ArtifactTailCatalog || footer.ArtifactID != reference.ArtifactID || footer.ContentLength != contentLength || footer.FileLength != reference.FileSize || footer.ContentSHA256 != reference.ContentSHA256 {
		return fail(fmt.Errorf("Tail Catalog Footer does not match Manifest"))
	}
	return &Catalog{file: file, reference: reference, header: header}, nil
}

func (c *Catalog) Header() format.TailCatalogHeader { return c.header }

func (c *Catalog) Lookup(streamID uint64) (format.TailSlot, bool, error) {
	if streamID >= c.header.SlotCount {
		return format.TailSlot{}, false, nil
	}
	position, err := format.TailSlotPosition(streamID)
	if err != nil {
		return format.TailSlot{}, false, err
	}
	encoded := make([]byte, format.TailSlotLength)
	if _, err = c.file.ReadAt(encoded, int64(position)); err != nil {
		return format.TailSlot{}, false, err
	}
	slot, err := format.UnmarshalTailSlot(encoded)
	if err != nil {
		return format.TailSlot{}, false, err
	}
	if !slot.Present {
		return format.TailSlot{}, false, nil
	}
	if slot.StreamID != streamID {
		return format.TailSlot{}, false, fmt.Errorf("Tail Slot %d contains Stream %d", streamID, slot.StreamID)
	}
	return slot, true, nil
}

func (c *Catalog) Close() error {
	if c.file == nil {
		return nil
	}
	err := c.file.Close()
	c.file = nil
	return err
}

func FindReference(manifest format.Manifest) (format.ArtifactReference, bool, error) {
	var found format.ArtifactReference
	for _, reference := range manifest.ArtifactReferences {
		if reference.ArtifactType != format.ArtifactTailCatalog {
			continue
		}
		if found.ArtifactID != (format.UUID{}) {
			return format.ArtifactReference{}, false, errors.New("Manifest contains multiple Tail Catalogs")
		}
		found = reference
	}
	return found, found.ArtifactID != (format.UUID{}), nil
}
