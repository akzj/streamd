package etcd

import (
	"testing"

	"github.com/akzj/streamd/internal/storage/format"
)

func TestParseNodeAndKeyAreCanonical(t *testing.T) {
	var id format.UUID
	id[15] = 1
	parsed, err := parseNode("00000000000000000000000000000001")
	if err != nil || parsed != id {
		t.Fatalf("parsed = %x, error = %v", parsed, err)
	}
	if _, err = parseNode("0000000000000000000000000000000A"); err == nil {
		t.Fatal("uppercase identity accepted")
	}
	coordinator := &Coordinator{prefix: "/streamd/v1"}
	if key := coordinator.key(id); key != "/streamd/v1/groups/00000000000000000000000000000001/leader" {
		t.Fatalf("key = %q", key)
	}
}
