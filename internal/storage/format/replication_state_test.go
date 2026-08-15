package format

import (
	"bytes"
	"errors"
	"testing"
)

func sampleReplicationState() ReplicationState {
	return ReplicationState{Header: ReplicationStateHeader{
		StateID:                testUUID(1),
		GroupID:                testUUID(2),
		NodeID:                 testUUID(3),
		Term:                   4,
		Role:                   ReplicationRolePrimary,
		Durability:             ReplicationDurabilityStrict,
		HasLeader:              true,
		LeaderID:               testUUID(3),
		HasLease:               true,
		LeaseExpiresAt:         1_700_000_000_000_000_000,
		LastAppended:           ReplicationPosition{Present: true, EntryID: 9, CRC32C: 109},
		LocalDurable:           ReplicationPosition{Present: true, EntryID: 9, CRC32C: 109},
		Replicated:             ReplicationPosition{Present: true, EntryID: 9, CRC32C: 109},
		Committed:              ReplicationPosition{Present: true, EntryID: 9, CRC32C: 109},
		Applied:                ReplicationPosition{Present: true, EntryID: 8, CRC32C: 108},
		EarliestWALEntryID:     8,
		HasInstalledSnapshot:   true,
		InstalledSnapshotID:    testUUID(4),
		InstalledSnapshotEntry: ReplicationPosition{Present: true, EntryID: 7, CRC32C: 107},
		CreatedAt:              1_699_999_999_000_000_000,
	}}
}

func TestReplicationStateGoldenAndRoundTrip(t *testing.T) {
	state := sampleReplicationState()
	encoded, err := MarshalReplicationState(state)
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "replication_state_v1.hex", encoded)
	decoded, err := UnmarshalReplicationState(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Header != state.Header || decoded.Footer.ArtifactType != ArtifactReplicationState || decoded.Footer.ArtifactID != state.Header.StateID {
		t.Fatalf("decoded State = %+v", decoded)
	}
	reencoded, err := MarshalReplicationState(decoded)
	if err != nil || !bytes.Equal(reencoded, encoded) {
		t.Fatalf("re-encode error = %v", err)
	}
}

func TestReplicationCurrentGoldenAndRoundTrip(t *testing.T) {
	stateBytes, err := MarshalReplicationState(sampleReplicationState())
	if err != nil {
		t.Fatal(err)
	}
	state, err := UnmarshalReplicationState(stateBytes)
	if err != nil {
		t.Fatal(err)
	}
	current := ReplicationCurrent{Generation: state.Header.Generation, StateID: state.Header.StateID, StateSHA256: state.Footer.ContentSHA256, StateFileName: ReplicationStateFileName(state.Header.Generation, state.Header.StateID)}
	encoded, err := MarshalReplicationCurrent(current)
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "replication_current_v1.hex", encoded)
	decoded, err := UnmarshalReplicationCurrent(encoded)
	if err != nil || decoded != current {
		t.Fatalf("decoded CURRENT = %+v, error = %v", decoded, err)
	}
}

func TestReplicationStateDetectsCorruption(t *testing.T) {
	encoded, err := MarshalReplicationState(sampleReplicationState())
	if err != nil {
		t.Fatal(err)
	}
	headerCorrupt := bytes.Clone(encoded)
	headerCorrupt[112] ^= 1
	if _, err = UnmarshalReplicationState(headerCorrupt); !errors.Is(err, ErrChecksum) {
		t.Fatalf("Header corruption = %v", err)
	}
	footerCorrupt := bytes.Clone(encoded)
	footerCorrupt[ReplicationStateHeaderLength+48] ^= 1
	if _, err = UnmarshalReplicationState(footerCorrupt); !errors.Is(err, ErrChecksum) {
		t.Fatalf("Footer corruption = %v", err)
	}
}

func TestReplicationStateRejectsInvalidInvariants(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*ReplicationStateHeader)
	}{
		{"applied ahead", func(h *ReplicationStateHeader) { h.Applied.EntryID = 10 }},
		{"checksum disagreement", func(h *ReplicationStateHeader) { h.Committed.CRC32C++ }},
		{"strict commit not replicated", func(h *ReplicationStateHeader) { h.Replicated.EntryID = 8; h.Replicated.CRC32C = 108 }},
		{"primary without lease", func(h *ReplicationStateHeader) { h.HasLease = false; h.LeaseExpiresAt = 0 }},
		{"snapshot too old", func(h *ReplicationStateHeader) { h.InstalledSnapshotEntry.EntryID = 6 }},
		{"generation gap", func(h *ReplicationStateHeader) { h.Generation = 2; h.PreviousGeneration = 0 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			header := sampleReplicationState().Header
			test.edit(&header)
			if _, err := MarshalReplicationStateHeader(header); !errors.Is(err, ErrInvalid) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestReplicationStateRoles(t *testing.T) {
	single := ReplicationStateHeader{StateID: testUUID(1), GroupID: testUUID(2), NodeID: testUUID(3), Role: ReplicationRoleSingle, Durability: ReplicationDurabilitySingleSync}
	if _, err := MarshalReplicationStateHeader(single); err != nil {
		t.Fatalf("SINGLE = %v", err)
	}
	standby := sampleReplicationState().Header
	standby.Role = ReplicationRoleStandby
	standby.NodeID = testUUID(5)
	standby.HasLease = false
	standby.LeaseExpiresAt = 0
	standby.Replicated = ReplicationPosition{}
	if _, err := MarshalReplicationStateHeader(standby); err != nil {
		t.Fatalf("STANDBY = %v", err)
	}
	recovering := standby
	recovering.Role = ReplicationRoleRecovering
	recovering.HasLeader = false
	recovering.LeaderID = UUID{}
	if _, err := MarshalReplicationStateHeader(recovering); err != nil {
		t.Fatalf("RECOVERING = %v", err)
	}
}

func FuzzUnmarshalReplicationState(f *testing.F) {
	seed, err := MarshalReplicationState(sampleReplicationState())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte("replication-state"))
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := UnmarshalReplicationState(data)
		if err != nil {
			return
		}
		encoded, err := MarshalReplicationState(decoded)
		if err != nil || !bytes.Equal(encoded, data) {
			t.Fatalf("round trip: %v", err)
		}
	})
}

func FuzzUnmarshalReplicationCurrent(f *testing.F) {
	digest := [32]byte{1}
	seed, err := MarshalReplicationCurrent(ReplicationCurrent{StateID: testUUID(1), StateSHA256: digest, StateFileName: "REPLICATION-STATE-00000000000000000000-01.bin"})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte("replication-current"))
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := UnmarshalReplicationCurrent(data)
		if err != nil {
			return
		}
		encoded, err := MarshalReplicationCurrent(decoded)
		if err != nil || !bytes.Equal(encoded, data) {
			t.Fatalf("round trip: %v", err)
		}
	})
}
