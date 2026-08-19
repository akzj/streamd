package segment

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/memtable"
)

type Metadata struct {
	Header      format.SegmentHeader
	Directories []format.StreamDirectoryEntry
	Footer      format.SegmentFooter
}

func WriteFile(path string, id format.UUID, createdAt int64, streams []memtable.StreamSnapshot) (Metadata, error) {
	var meta Metadata
	if len(streams) == 0 {
		return meta, fmt.Errorf("cannot build empty Segment")
	}
	streams = slices.Clone(streams)
	slices.SortFunc(streams, func(a, b memtable.StreamSnapshot) int {
		if a.StreamID < b.StreamID {
			return -1
		}
		if a.StreamID > b.StreamID {
			return 1
		}
		return 0
	})
	var records uint64
	var dataLength uint64
	var firstID, lastID uint64
	decoded := make([][]format.RecordFrame, len(streams))
	for si, s := range streams {
		if len(s.Frames) == 0 {
			return meta, fmt.Errorf("Stream %d is empty", s.StreamID)
		}
		decoded[si] = make([]format.RecordFrame, len(s.Frames))
		for i, frame := range s.Frames {
			r, err := format.UnmarshalRecordFrame(frame)
			if err != nil {
				return meta, err
			}
			if r.StreamID != s.StreamID {
				return meta, fmt.Errorf("Frame Stream ID mismatch")
			}
			if i > 0 {
				p := decoded[si][i-1]
				if r.Sequence != p.Sequence+1 || r.ByteOffset != p.ByteOffset+uint64(len(s.Frames[i-1])) || r.RecordedAt < p.RecordedAt {
					return meta, fmt.Errorf("Stream frames are not continuous")
				}
			}
			decoded[si][i] = r
			if records == 0 || r.EntryID < firstID {
				firstID = r.EntryID
			}
			if records == 0 || r.EntryID > lastID {
				lastID = r.EntryID
			}
			records++
			dataLength += uint64(len(frame))
		}
	}
	header, err := format.NewSegmentHeader(id, createdAt, firstID, lastID, uint64(len(streams)), records, dataLength)
	if err != nil {
		return meta, err
	}
	dirs := make([]format.StreamDirectoryEntry, len(streams))
	indexPos, dataPos := header.IndexOffset, header.DataOffset
	for si, s := range streams {
		rs := decoded[si]
		var streamBytes uint64
		minID, maxID := rs[0].EntryID, rs[0].EntryID
		for i, r := range rs {
			streamBytes += uint64(len(s.Frames[i]))
			if r.EntryID < minID {
				minID = r.EntryID
			}
			if r.EntryID > maxID {
				maxID = r.EntryID
			}
		}
		dirs[si] = format.StreamDirectoryEntry{StreamID: s.StreamID, FirstSequence: rs[0].Sequence, RecordCount: uint64(len(rs)), FirstByteOffset: rs[0].ByteOffset, NextByteOffset: rs[0].ByteOffset + streamBytes, FirstRecordedAt: rs[0].RecordedAt, LastRecordedAt: rs[len(rs)-1].RecordedAt, FirstEntryID: minID, LastEntryID: maxID, RecordIndexOffset: indexPos, RecordIndexLength: uint64(len(rs)) * format.DenseIndexEntryLength, StreamDataOffset: dataPos, StreamDataLength: streamBytes}
		indexPos += dirs[si].RecordIndexLength
		dataPos += streamBytes
	}
	if err = format.ValidateSegmentLayout(header, dirs); err != nil {
		return meta, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0640)
	if err != nil {
		return meta, err
	}
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(path)
		}
	}()
	if err = f.Truncate(int64(header.FooterOffset + format.SegmentFooterSectionLength)); err != nil {
		return meta, err
	}
	hb, _ := format.MarshalSegmentHeader(header)
	if err = fsutil.WriteFullAt(f, hb, 0); err != nil {
		return meta, err
	}
	for i, d := range dirs {
		b, _ := format.MarshalStreamDirectoryEntry(d)
		if err = fsutil.WriteFullAt(f, b, int64(header.DirectoryOffset)+int64(i*format.StreamDirectoryEntryLength)); err != nil {
			return meta, err
		}
	}
	for si, s := range streams {
		relative := uint64(0)
		for i, r := range decoded[si] {
			idx := format.DenseIndexEntry{RelativeByteOffset: relative, RecordedAtDelta: uint64(r.RecordedAt - decoded[si][0].RecordedAt), FrameLength: uint32(len(s.Frames[i])), FrameCRC32C: binary.LittleEndian.Uint32(s.Frames[i][len(s.Frames[i])-4:])}
			b, _ := format.MarshalDenseIndexEntry(idx)
			offset := dirs[si].RecordIndexOffset + uint64(i*format.DenseIndexEntryLength)
			if err = fsutil.WriteFullAt(f, b, int64(offset)); err != nil {
				return meta, err
			}
			if err = fsutil.WriteFullAt(f, s.Frames[i], int64(dirs[si].StreamDataOffset+relative)); err != nil {
				return meta, err
			}
			relative += uint64(len(s.Frames[i]))
		}
	}
	if err = f.Sync(); err != nil {
		return meta, err
	}
	h := sha256.New()
	if _, err = io.CopyN(h, io.NewSectionReader(f, 0, int64(header.FooterOffset)), int64(header.FooterOffset)); err != nil {
		return meta, err
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	footer := format.SegmentFooter{SegmentID: id, FileLength: header.FooterOffset + format.SegmentFooterSectionLength, ContentLength: header.FooterOffset, StreamCount: header.StreamCount, RecordCount: header.RecordCount, ContentSHA256: digest}
	fb, _ := format.MarshalSegmentFooter(footer)
	if err = fsutil.WriteFullAt(f, fb, int64(header.FooterOffset)); err != nil {
		return meta, err
	}
	if err = f.Sync(); err != nil {
		return meta, err
	}
	if err = fsutil.SyncDir(filepath.Dir(path)); err != nil {
		return meta, err
	}
	ok = true
	meta = Metadata{Header: header, Directories: dirs, Footer: footer}
	return meta, nil
}

type Reader struct {
	file        *os.File
	Header      format.SegmentHeader
	Directories []format.StreamDirectoryEntry
	Footer      format.SegmentFooter
}

func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fail := func(e error) (*Reader, error) { f.Close(); return nil, e }
	section := make([]byte, format.SegmentSectionAlignment)
	if _, err = io.ReadFull(io.NewSectionReader(f, 0, int64(len(section))), section); err != nil {
		return fail(err)
	}
	header, err := format.UnmarshalSegmentHeaderSection(section)
	if err != nil {
		return fail(err)
	}
	info, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	if uint64(info.Size()) != header.FooterOffset+format.SegmentFooterSectionLength {
		return fail(fmt.Errorf("Segment file length mismatch"))
	}
	footerSection := make([]byte, format.SegmentFooterSectionLength)
	if _, err = f.ReadAt(footerSection, int64(header.FooterOffset)); err != nil {
		return fail(err)
	}
	footer, err := format.UnmarshalSegmentFooter(footerSection[:format.SegmentFooterLength])
	if err != nil {
		return fail(err)
	}
	for _, value := range footerSection[format.SegmentFooterLength:] {
		if value != 0 {
			return fail(fmt.Errorf("Segment Footer padding is not zero"))
		}
	}
	if footer.SegmentID != header.SegmentID || footer.FileLength != uint64(info.Size()) || footer.ContentLength != header.FooterOffset || footer.RecordCount != header.RecordCount || footer.StreamCount != header.StreamCount {
		return fail(fmt.Errorf("Segment Footer does not match Header"))
	}
	dirs := make([]format.StreamDirectoryEntry, header.StreamCount)
	for i := range dirs {
		b := make([]byte, format.StreamDirectoryEntryLength)
		if _, err = f.ReadAt(b, int64(header.DirectoryOffset)+int64(i*format.StreamDirectoryEntryLength)); err != nil {
			return fail(err)
		}
		dirs[i], err = format.UnmarshalStreamDirectoryEntry(b)
		if err != nil {
			return fail(err)
		}
	}
	if err = format.ValidateSegmentLayout(header, dirs); err != nil {
		return fail(err)
	}
	return &Reader{file: f, Header: header, Directories: dirs, Footer: footer}, nil
}
func (r *Reader) Close() error { return r.file.Close() }
func (r *Reader) Read(streamID, sequence uint64) (format.RecordFrame, error) {
	frame, err := r.ReadFrame(streamID, sequence)
	if err != nil {
		return format.RecordFrame{}, err
	}
	return format.UnmarshalRecordFrame(frame)
}
func (r *Reader) ReadFrame(streamID, sequence uint64) ([]byte, error) {
	i, ok := slices.BinarySearchFunc(r.Directories, streamID, func(d format.StreamDirectoryEntry, id uint64) int {
		if d.StreamID < id {
			return -1
		}
		if d.StreamID > id {
			return 1
		}
		return 0
	})
	if !ok {
		return nil, fmt.Errorf("Stream %d not found", streamID)
	}
	d := r.Directories[i]
	if sequence < d.FirstSequence || sequence >= d.FirstSequence+d.RecordCount {
		return nil, fmt.Errorf("Sequence outside Segment extent")
	}
	ordinal := sequence - d.FirstSequence
	b := make([]byte, format.DenseIndexEntryLength)
	if _, err := r.file.ReadAt(b, int64(d.RecordIndexOffset+ordinal*format.DenseIndexEntryLength)); err != nil {
		return nil, err
	}
	idx, err := format.UnmarshalDenseIndexEntry(b)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, idx.FrameLength)
	if _, err = r.file.ReadAt(frame, int64(d.StreamDataOffset+idx.RelativeByteOffset)); err != nil {
		return nil, err
	}
	record, err := format.UnmarshalRecordFrame(frame)
	if err != nil {
		return nil, err
	}
	if record.StreamID != streamID || record.Sequence != sequence || record.ByteOffset != d.FirstByteOffset+idx.RelativeByteOffset || record.RecordedAt != d.FirstRecordedAt+int64(idx.RecordedAtDelta) || binary.LittleEndian.Uint32(frame[len(frame)-4:]) != idx.FrameCRC32C {
		return nil, fmt.Errorf("Segment Index does not match Frame")
	}
	return frame, nil
}

func ScrubFile(path string) (Metadata, error) {
	reader, err := Open(path)
	if err != nil {
		return Metadata{}, err
	}
	defer reader.Close()
	hash := sha256.New()
	if _, err = io.CopyN(hash, io.NewSectionReader(reader.file, 0, int64(reader.Footer.ContentLength)), int64(reader.Footer.ContentLength)); err != nil {
		return Metadata{}, err
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	if digest != reader.Footer.ContentSHA256 {
		return Metadata{}, fmt.Errorf("Segment content SHA-256 mismatch")
	}
	var records uint64
	for _, directory := range reader.Directories {
		for sequence := directory.FirstSequence; sequence < directory.FirstSequence+directory.RecordCount; sequence++ {
			if _, err = reader.ReadFrame(directory.StreamID, sequence); err != nil {
				return Metadata{}, err
			}
			records++
		}
	}
	if records != reader.Header.RecordCount {
		return Metadata{}, fmt.Errorf("Segment scrub Record count mismatch")
	}
	return Metadata{Header: reader.Header, Directories: slices.Clone(reader.Directories), Footer: reader.Footer}, nil
}
