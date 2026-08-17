package format

import (
	"bytes"
	"slices"
)

type LocatorSnapshot struct {
	Header LocatorSnapshotHeader
	Packs  []LocatorPackReference
	Roots  []LocatorRootEntry
	Footer ArtifactFooter
}

func MarshalLocatorSnapshot(s LocatorSnapshot) ([]byte, error) {
	packs := slices.Clone(s.Packs)
	roots := slices.Clone(s.Roots)
	slices.SortFunc(packs, func(a, b LocatorPackReference) int { return bytes.Compare(a.PackID[:], b.PackID[:]) })
	slices.SortFunc(roots, func(a, b LocatorRootEntry) int { return cmpU64(a.StreamID, b.StreamID) })
	if err := validateLocatorOrder(packs, roots); err != nil {
		return nil, err
	}
	s.Header.PackCount = uint32(len(packs))
	s.Header.RootCount = uint32(len(roots))
	if int(s.Header.PackCount) != len(packs) || int(s.Header.RootCount) != len(roots) {
		return nil, fmtTooLarge("Locator Snapshot count", len(packs)+len(roots), ^uint32(0))
	}
	content, err := MarshalLocatorSnapshotHeader(s.Header)
	if err != nil {
		return nil, err
	}
	for _, p := range packs {
		x, e := MarshalLocatorPackReference(p)
		if e != nil {
			return nil, e
		}
		content = append(content, x...)
	}
	for _, r := range roots {
		x, e := MarshalLocatorRootEntry(r)
		if e != nil {
			return nil, e
		}
		content = append(content, x...)
	}
	f, _ := NewArtifactFooter(ArtifactLocatorSnapshot, s.Header.ArtifactID, content)
	fb, _ := MarshalArtifactFooter(f)
	return append(content, fb...), nil
}
func UnmarshalLocatorSnapshot(b []byte) (LocatorSnapshot, error) {
	var s LocatorSnapshot
	if len(b) < LocatorSnapshotHeaderLength+ArtifactFooterLength {
		return s, truncatedf("Locator Snapshot is truncated")
	}
	h, err := UnmarshalLocatorSnapshotHeader(b[:LocatorSnapshotHeaderLength])
	if err != nil {
		return s, err
	}
	pos := LocatorSnapshotHeaderLength
	s.Packs = make([]LocatorPackReference, 0, int(h.PackCount))
	for i := uint32(0); i < h.PackCount; i++ {
		entry, next, e := nextReference(b, pos, locatorPackReferenceFixedLength, "Locator Pack")
		if e != nil {
			return s, e
		}
		p, e := UnmarshalLocatorPackReference(entry)
		if e != nil {
			return s, e
		}
		s.Packs = append(s.Packs, p)
		pos = next
	}
	needed := uint64(h.RootCount) * LocatorRootEntryLength
	if uint64(len(b)-pos) < needed+ArtifactFooterLength {
		return s, truncatedf("Locator Roots are truncated")
	}
	s.Roots = make([]LocatorRootEntry, 0, int(h.RootCount))
	for i := uint32(0); i < h.RootCount; i++ {
		r, e := UnmarshalLocatorRootEntry(b[pos : pos+LocatorRootEntryLength])
		if e != nil {
			return s, e
		}
		s.Roots = append(s.Roots, r)
		pos += LocatorRootEntryLength
	}
	if len(b)-pos != ArtifactFooterLength {
		return s, invalidf("Locator Snapshot has unexpected bytes")
	}
	f, e := VerifyArtifact(b[:pos], b[pos:], ArtifactLocatorSnapshot, h.ArtifactID)
	if e != nil {
		return s, e
	}
	if e = validateLocatorOrder(s.Packs, s.Roots); e != nil {
		return s, e
	}
	s.Header = h
	s.Footer = f
	return s, nil
}
func validateLocatorOrder(p []LocatorPackReference, r []LocatorRootEntry) error {
	packs := make(map[UUID]struct{}, len(p))
	for i := 1; i < len(p); i++ {
		if bytes.Compare(p[i-1].PackID[:], p[i].PackID[:]) >= 0 {
			return invalidf("Locator Packs are not strictly sorted")
		}
	}
	for _, reference := range p {
		packs[reference.PackID] = struct{}{}
	}
	for i := 1; i < len(r); i++ {
		if r[i-1].StreamID >= r[i].StreamID {
			return invalidf("Locator Roots are not strictly sorted")
		}
	}
	for _, root := range r {
		if _, ok := packs[root.PackID]; !ok {
			return invalidf("Locator Root references an unknown Pack")
		}
	}
	return nil
}

const RegistryBlockEntriesV1 = 256

type RegistrySnapshot struct {
	Header  RegistrySnapshotHeader
	Entries []RegistryEntry
	Footer  ArtifactFooter
}

func MarshalRegistrySnapshot(s RegistrySnapshot) ([]byte, error) {
	entries := slices.Clone(s.Entries)
	slices.SortFunc(entries, compareRegistryEntry)
	if err := validateRegistryMappings(entries); err != nil {
		return nil, err
	}
	encoded := make([][]byte, len(entries))
	for i, e := range entries {
		x, err := MarshalRegistryEntry(e)
		if err != nil {
			return nil, err
		}
		if e.CreatedEntryID > s.Header.CoveredEntryID {
			return nil, invalidf("Registry Entry is after checkpoint")
		}
		encoded[i] = x
	}
	blocks := (len(entries) + RegistryBlockEntriesV1 - 1) / RegistryBlockEntriesV1
	s.Header.EntryCount = uint64(len(entries))
	s.Header.BlockCount = uint32(blocks)
	s.Header.BlockIndexOffset = RegistrySnapshotHeaderLength
	indexLen := 0
	for i := 0; i < blocks; i++ {
		e := entries[i*RegistryBlockEntriesV1]
		indexLen += registryBlockIndexFixedLength + len(e.Namespace) + len(e.StreamName)
	}
	s.Header.EntriesOffset = uint64(RegistrySnapshotHeaderLength + indexLen)
	header, err := MarshalRegistrySnapshotHeader(s.Header)
	if err != nil {
		return nil, err
	}
	content := header
	entryOffset := s.Header.EntriesOffset
	for i := 0; i < blocks; i++ {
		start := i * RegistryBlockEntriesV1
		end := min(start+RegistryBlockEntriesV1, len(entries))
		idx := RegistryBlockIndexEntry{EntryCount: uint32(end - start), EntriesOffset: entryOffset, FirstNamespace: entries[start].Namespace, FirstStreamName: entries[start].StreamName}
		x, _ := MarshalRegistryBlockIndexEntry(idx)
		content = append(content, x...)
		for _, x := range encoded[start:end] {
			entryOffset += uint64(len(x))
		}
	}
	for _, x := range encoded {
		content = append(content, x...)
	}
	f, _ := NewArtifactFooter(ArtifactRegistrySnapshot, s.Header.ArtifactID, content)
	fb, _ := MarshalArtifactFooter(f)
	return append(content, fb...), nil
}
func UnmarshalRegistrySnapshot(b []byte) (RegistrySnapshot, error) {
	var s RegistrySnapshot
	if len(b) < RegistrySnapshotHeaderLength+ArtifactFooterLength {
		return s, truncatedf("Registry Snapshot is truncated")
	}
	h, err := UnmarshalRegistrySnapshotHeader(b[:RegistrySnapshotHeaderLength])
	if err != nil {
		return s, err
	}
	pos := RegistrySnapshotHeaderLength
	blocks := make([]RegistryBlockIndexEntry, 0, int(h.BlockCount))
	for i := uint32(0); i < h.BlockCount; i++ {
		entry, next, e := nextReference(b, pos, registryBlockIndexFixedLength, "Registry Block")
		if e != nil {
			return s, e
		}
		idx, e := UnmarshalRegistryBlockIndexEntry(entry)
		if e != nil {
			return s, e
		}
		if idx.EntryCount > RegistryBlockEntriesV1 {
			return s, invalidf("Registry Block exceeds entry limit")
		}
		blocks = append(blocks, idx)
		pos = next
	}
	if uint64(pos) != h.EntriesOffset {
		return s, invalidf("Registry entries_offset is invalid")
	}
	s.Entries = make([]RegistryEntry, 0, int(h.EntryCount))
	for _, block := range blocks {
		if block.EntriesOffset != uint64(pos) {
			return s, invalidf("Registry Block offset is invalid")
		}
		for j := uint32(0); j < block.EntryCount; j++ {
			entry, next, e := nextReference(b, pos, registryEntryFixedLength, "Registry Entry")
			if e != nil {
				return s, e
			}
			re, e := UnmarshalRegistryEntry(entry)
			if e != nil {
				return s, e
			}
			if re.CreatedEntryID > h.CoveredEntryID {
				return s, invalidf("Registry Entry is after checkpoint")
			}
			if j == 0 && (re.Namespace != block.FirstNamespace || re.StreamName != block.FirstStreamName) {
				return s, invalidf("Registry Block first key mismatch")
			}
			s.Entries = append(s.Entries, re)
			pos = next
		}
	}
	if uint64(len(s.Entries)) != h.EntryCount || len(b)-pos != ArtifactFooterLength {
		return s, invalidf("Registry Snapshot counts or trailing bytes are invalid")
	}
	if err := validateRegistryMappings(s.Entries); err != nil {
		return s, err
	}
	f, e := VerifyArtifact(b[:pos], b[pos:], ArtifactRegistrySnapshot, h.ArtifactID)
	if e != nil {
		return s, e
	}
	s.Header = h
	s.Footer = f
	return s, nil
}
func compareRegistryEntry(a, b RegistryEntry) int {
	if c := bytes.Compare([]byte(a.Namespace), []byte(b.Namespace)); c != 0 {
		return c
	}
	return bytes.Compare([]byte(a.StreamName), []byte(b.StreamName))
}
func validateRegistryMappings(entries []RegistryEntry) error {
	ids := make(map[uint64]struct{}, len(entries))
	for i, e := range entries {
		if i > 0 && compareRegistryEntry(entries[i-1], e) >= 0 {
			return invalidf("Registry keys are duplicate or unsorted")
		}
		if _, ok := ids[e.StreamID]; ok {
			return invalidf("Registry Stream ID maps to multiple names")
		}
		ids[e.StreamID] = struct{}{}
	}
	return nil
}

type SnapshotManifest struct {
	Header    SnapshotHeader
	Artifacts []SnapshotArtifact
	Footer    ArtifactFooter
}

func MarshalSnapshotManifest(s SnapshotManifest) ([]byte, error) {
	artifacts := slices.Clone(s.Artifacts)
	slices.SortFunc(artifacts, compareSnapshotArtifact)
	for i := 1; i < len(artifacts); i++ {
		if compareSnapshotArtifact(artifacts[i-1], artifacts[i]) >= 0 {
			return nil, invalidf("Snapshot Artifacts are duplicate")
		}
	}
	s.Header.ArtifactCount = uint64(len(artifacts))
	content, err := MarshalSnapshotHeader(s.Header)
	if err != nil {
		return nil, err
	}
	for _, a := range artifacts {
		x, e := MarshalSnapshotArtifact(a)
		if e != nil {
			return nil, e
		}
		content = append(content, x...)
	}
	f, _ := NewArtifactFooter(ArtifactSnapshotManifest, s.Header.SnapshotID, content)
	fb, _ := MarshalArtifactFooter(f)
	return append(content, fb...), nil
}
func UnmarshalSnapshotManifest(b []byte) (SnapshotManifest, error) {
	var s SnapshotManifest
	if len(b) < SnapshotHeaderLength+ArtifactFooterLength {
		return s, truncatedf("Snapshot Manifest is truncated")
	}
	h, err := UnmarshalSnapshotHeader(b[:SnapshotHeaderLength])
	if err != nil {
		return s, err
	}
	pos := SnapshotHeaderLength
	if h.ArtifactCount > uint64((len(b)-pos-ArtifactFooterLength)/snapshotArtifactFixedLength) {
		return s, truncatedf("Snapshot Artifact count exceeds file")
	}
	s.Artifacts = make([]SnapshotArtifact, 0, int(h.ArtifactCount))
	for i := uint64(0); i < h.ArtifactCount; i++ {
		entry, next, e := nextReference(b, pos, snapshotArtifactFixedLength, "Snapshot Artifact")
		if e != nil {
			return s, e
		}
		a, e := UnmarshalSnapshotArtifact(entry)
		if e != nil {
			return s, e
		}
		s.Artifacts = append(s.Artifacts, a)
		pos = next
	}
	if len(b)-pos != ArtifactFooterLength {
		return s, invalidf("Snapshot Manifest has unexpected bytes")
	}
	for i := 1; i < len(s.Artifacts); i++ {
		if compareSnapshotArtifact(s.Artifacts[i-1], s.Artifacts[i]) >= 0 {
			return s, invalidf("Snapshot Artifacts are unsorted")
		}
	}
	f, e := VerifyArtifact(b[:pos], b[pos:], ArtifactSnapshotManifest, h.SnapshotID)
	if e != nil {
		return s, e
	}
	s.Header = h
	s.Footer = f
	return s, nil
}
func compareSnapshotArtifact(a, b SnapshotArtifact) int {
	if a.ArtifactType < b.ArtifactType {
		return -1
	}
	if a.ArtifactType > b.ArtifactType {
		return 1
	}
	return bytes.Compare(a.ArtifactID[:], b.ArtifactID[:])
}
func cmpU64(a, b uint64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
