package manifest

import (
	"errors"
	"fmt"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"os"
	"path/filepath"
	"sync"
)

type Store struct {
	mu      sync.RWMutex
	root    string
	current *format.Manifest
	name    string
}

func Open(root string) (*Store, error) {
	s := &Store{root: root}
	pointerBytes, err := os.ReadFile(filepath.Join(root, "CURRENT"))
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	pointer, err := format.UnmarshalCurrentPointer(pointerBytes)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filepath.Join(root, "manifests", pointer.ManifestFileName))
	if err != nil {
		return nil, err
	}
	manifest, err := format.UnmarshalManifest(data)
	if err != nil {
		return nil, err
	}
	if manifest.Header.Generation != pointer.Generation || manifest.Header.FileID != pointer.ManifestFileID || manifest.Footer.ContentSHA256 != pointer.ManifestSHA256 {
		return nil, fmt.Errorf("CURRENT does not match Manifest")
	}
	s.current = &manifest
	s.name = pointer.ManifestFileName
	return s, nil
}
func (s *Store) Current() (format.Manifest, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.current == nil {
		return format.Manifest{}, false
	}
	return cloneManifest(*s.current), true
}
func (s *Store) Publish(next format.Manifest) (format.Manifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		if next.Header.Generation != 0 || next.Header.PreviousGeneration != 0 || next.Header.PreviousManifestSHA256 != [32]byte{} {
			return format.Manifest{}, fmt.Errorf("initial Manifest must be Generation 0")
		}
	} else {
		want := s.current.Header.Generation + 1
		if next.Header.Generation != want || next.Header.PreviousGeneration != s.current.Header.Generation || next.Header.PreviousManifestSHA256 != s.current.Footer.ContentSHA256 {
			return format.Manifest{}, fmt.Errorf("Manifest does not continue current Generation")
		}
	}
	encoded, err := format.MarshalManifest(next)
	if err != nil {
		return format.Manifest{}, err
	}
	verified, err := format.UnmarshalManifest(encoded)
	if err != nil {
		return format.Manifest{}, err
	}
	name := fmt.Sprintf("MANIFEST-%020d-%x.bin", verified.Header.Generation, verified.Header.FileID)
	manifestDir := filepath.Join(s.root, "manifests")
	if err = fsutil.AtomicWrite(manifestDir, name, encoded, 0640, nil); err != nil {
		return format.Manifest{}, err
	}
	pointer := format.CurrentPointer{Generation: verified.Header.Generation, ManifestFileID: verified.Header.FileID, ManifestSHA256: verified.Footer.ContentSHA256, ManifestFileName: name}
	pointerBytes, err := format.MarshalCurrentPointer(pointer)
	if err != nil {
		return format.Manifest{}, err
	}
	if err = fsutil.AtomicWrite(s.root, "CURRENT", pointerBytes, 0640, nil); err != nil {
		return format.Manifest{}, err
	}
	s.current = &verified
	s.name = name
	return cloneManifest(verified), nil
}
func cloneManifest(m format.Manifest) format.Manifest {
	m.SegmentReferences = append([]format.SegmentReference(nil), m.SegmentReferences...)
	m.ArtifactReferences = append([]format.ArtifactReference(nil), m.ArtifactReferences...)
	return m
}
