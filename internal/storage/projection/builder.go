package projection

import (
	"github.com/akzj/streamd/internal/storage/format"
	locatorstore "github.com/akzj/streamd/internal/storage/locator"
	"github.com/akzj/streamd/internal/storage/registry"
	"github.com/akzj/streamd/internal/storage/segment"
)

type Build struct {
	TailReference     format.ArtifactReference
	RegistryReference format.ArtifactReference
	Locator           locatorstore.BuildResult
}

func BuildReferences(root string, generation, coveredEntryID uint64, createdAt int64, descriptors []segment.Descriptor) (Build, error) {
	tailID, err := locatorstore.NewID()
	if err != nil {
		return Build{}, err
	}
	snapshotID, err := locatorstore.NewID()
	if err != nil {
		return Build{}, err
	}
	packID, err := locatorstore.NewID()
	if err != nil {
		return Build{}, err
	}
	registryID, err := locatorstore.NewID()
	if err != nil {
		return Build{}, err
	}
	locatorResult, err := locatorstore.BuildCheckpoint(root, snapshotID, packID, tailID, generation, coveredEntryID, descriptors)
	if err != nil {
		return Build{}, err
	}
	registryReference, err := registry.BuildCheckpointFromSegments(root, registryID, coveredEntryID, createdAt, descriptors)
	if err != nil {
		return Build{}, err
	}
	return Build{TailReference: locatorResult.TailReference, RegistryReference: registryReference, Locator: locatorResult}, nil
}

func ReplacedArtifacts(previous []format.ArtifactReference, oldLocator *locatorstore.Store, replacement Build) []format.ArtifactReference {
	var retired []format.ArtifactReference
	for _, old := range previous {
		if (old.ArtifactType == format.ArtifactTailCatalog && old.ArtifactID != replacement.TailReference.ArtifactID) ||
			(old.ArtifactType == format.ArtifactLocatorSnapshot && old.ArtifactID != replacement.Locator.Reference.ArtifactID) ||
			(old.ArtifactType == format.ArtifactRegistrySnapshot && old.ArtifactID != replacement.RegistryReference.ArtifactID) {
			retired = append(retired, old)
		}
	}
	if oldLocator != nil {
		retired = append(retired, oldLocator.PackArtifacts()...)
	}
	return retired
}
