package identity

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
)

func Ensure(root string, desired format.NodeIdentity) (format.NodeIdentity, error) {
	path := filepath.Join(root, "NODE")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		encoded, marshalErr := format.MarshalNodeIdentity(desired)
		if marshalErr != nil {
			return format.NodeIdentity{}, marshalErr
		}
		if err = fsutil.AtomicWrite(root, "NODE", encoded, 0640, nil); err != nil {
			return format.NodeIdentity{}, err
		}
		return desired, nil
	}
	if err != nil {
		return format.NodeIdentity{}, err
	}
	current, err := format.UnmarshalNodeIdentity(data)
	if err != nil {
		return format.NodeIdentity{}, err
	}
	if current.ClusterID != desired.ClusterID || current.GroupID != desired.GroupID || current.NodeID != desired.NodeID {
		return format.NodeIdentity{}, fmt.Errorf("configured identity does not match NODE")
	}
	return current, nil
}

func Load(root string) (format.NodeIdentity, error) {
	data, err := os.ReadFile(filepath.Join(root, "NODE"))
	if err != nil {
		return format.NodeIdentity{}, err
	}
	return format.UnmarshalNodeIdentity(data)
}
