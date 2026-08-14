package engine

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/akzj/streamd/internal/storage/format"
)

func TestCheckpointCrashRecovery(t *testing.T) {
	if os.Getenv("STREAMD_CRASH_HELPER") == "1" {
		checkpointCrashHelper()
		return
	}
	for _, point := range []string{"after_wal_rotate", "after_manifest_publish", "after_view_install"} {
		t.Run(point, func(t *testing.T) {
			data := t.TempDir()
			command := exec.Command(os.Args[0], "-test.run=TestCheckpointCrashRecovery")
			command.Env = append(os.Environ(), "STREAMD_CRASH_HELPER=1", "STREAMD_CRASH_POINT="+point, "STREAMD_CRASH_DATA="+data)
			err := command.Run()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 86 {
				t.Fatalf("helper exit = %v", err)
			}
			store, openErr := OpenWithIdentity(data, crashIdentity())
			if openErr != nil {
				t.Fatal(openErr)
			}
			defer store.Close()
			result, readErr := store.Read("n", "s", 0, 10, 0)
			if readErr != nil || len(result.Records) != 1 || string(result.Records[0].Payload) != "durable" {
				t.Fatalf("recovered Read = %+v, error = %v", result, readErr)
			}
		})
	}
}

func checkpointCrashHelper() {
	store, err := OpenWithIdentity(os.Getenv("STREAMD_CRASH_DATA"), crashIdentity())
	if err != nil {
		os.Exit(80)
	}
	_, err = store.Append(context.Background(), AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("request"), Producer: "test", Records: []InputRecord{{Payload: []byte("durable")}}})
	if err != nil {
		os.Exit(81)
	}
	point := os.Getenv("STREAMD_CRASH_POINT")
	store.checkpointHook = func(current string) error {
		if current == point {
			os.Exit(86)
		}
		return nil
	}
	if _, _, err = store.Checkpoint(); err != nil {
		os.Exit(82)
	}
	os.Exit(83)
}

func crashIdentity() format.NodeIdentity {
	var identity format.NodeIdentity
	identity.ClusterID[15] = 1
	identity.GroupID[15] = 2
	identity.NodeID[15] = 3
	identity.CreatedAt = 1
	return identity
}
