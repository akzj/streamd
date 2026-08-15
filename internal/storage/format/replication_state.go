package format

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"
)

var (
	replicationStateMagic   = [8]byte{'S', 'T', 'R', 'M', 'R', 'S', 'T', '1'}
	replicationCurrentMagic = [8]byte{'S', 'T', 'R', 'M', 'R', 'S', 'C', '1'}
)

const (
	ReplicationStateHasLastAppended uint32 = 1 << iota
	ReplicationStateHasLocalDurable
	ReplicationStateHasReplicated
	ReplicationStateHasCommitted
	ReplicationStateHasApplied
	ReplicationStateHasLeader
	ReplicationStateHasLease
	ReplicationStateHasInstalledSnapshot
	replicationStateKnownFlags = ReplicationStateHasLastAppended |
		ReplicationStateHasLocalDurable |
		ReplicationStateHasReplicated |
		ReplicationStateHasCommitted |
		ReplicationStateHasApplied |
		ReplicationStateHasLeader |
		ReplicationStateHasLease |
		ReplicationStateHasInstalledSnapshot
	replicationCurrentFixedLength = 80
)

type ReplicationRole uint16

const (
	ReplicationRoleSingle ReplicationRole = iota + 1
	ReplicationRolePrimary
	ReplicationRoleStandby
	ReplicationRoleRecovering
)

type ReplicationDurability uint16

const (
	ReplicationDurabilitySingleSync ReplicationDurability = iota + 1
	ReplicationDurabilityStrict
	ReplicationDurabilityDegraded
)

type ReplicationPosition struct {
	Present bool
	EntryID uint64
	CRC32C  uint32
}

type ReplicationStateHeader struct {
	StateID                UUID
	Generation             uint64
	PreviousGeneration     uint64
	PreviousStateSHA256    [sha256.Size]byte
	GroupID                UUID
	NodeID                 UUID
	Term                   uint64
	Role                   ReplicationRole
	Durability             ReplicationDurability
	HasLeader              bool
	LeaderID               UUID
	HasLease               bool
	LeaseExpiresAt         int64
	LastAppended           ReplicationPosition
	LocalDurable           ReplicationPosition
	Replicated             ReplicationPosition
	Committed              ReplicationPosition
	Applied                ReplicationPosition
	EarliestWALEntryID     uint64
	HasInstalledSnapshot   bool
	InstalledSnapshotID    UUID
	InstalledSnapshotEntry ReplicationPosition
	CreatedAt              int64
}

type ReplicationState struct {
	Header ReplicationStateHeader
	Footer ArtifactFooter
}

type ReplicationCurrent struct {
	Generation    uint64
	StateID       UUID
	StateSHA256   [sha256.Size]byte
	StateFileName string
}

func MarshalReplicationStateHeader(header ReplicationStateHeader) ([]byte, error) {
	if err := validateReplicationStateHeader(header); err != nil {
		return nil, err
	}
	encoded := make([]byte, ReplicationStateHeaderLength)
	copy(encoded[:8], replicationStateMagic[:])
	putU16(encoded[8:10], VersionV1)
	putU16(encoded[10:12], ReplicationStateHeaderLength)
	putU32(encoded[12:16], replicationStateFlags(header))
	copy(encoded[16:32], header.StateID[:])
	putU64(encoded[32:40], header.Generation)
	putU64(encoded[40:48], header.PreviousGeneration)
	copy(encoded[48:80], header.PreviousStateSHA256[:])
	copy(encoded[80:96], header.GroupID[:])
	copy(encoded[96:112], header.NodeID[:])
	putU64(encoded[112:120], header.Term)
	putU16(encoded[120:122], uint16(header.Role))
	putU16(encoded[122:124], uint16(header.Durability))
	copy(encoded[128:144], header.LeaderID[:])
	putI64(encoded[144:152], header.LeaseExpiresAt)
	putReplicationPosition(encoded[152:168], header.LastAppended)
	putReplicationPosition(encoded[168:184], header.LocalDurable)
	putReplicationPosition(encoded[184:200], header.Replicated)
	putReplicationPosition(encoded[200:216], header.Committed)
	putReplicationPosition(encoded[216:232], header.Applied)
	putU64(encoded[232:240], header.EarliestWALEntryID)
	copy(encoded[240:256], header.InstalledSnapshotID[:])
	putU64(encoded[256:264], header.InstalledSnapshotEntry.EntryID)
	putU32(encoded[264:268], header.InstalledSnapshotEntry.CRC32C)
	putI64(encoded[272:280], header.CreatedAt)
	putU32(encoded[316:320], checksum(encoded[:316]))
	return encoded, nil
}

func UnmarshalReplicationStateHeader(encoded []byte) (ReplicationStateHeader, error) {
	var header ReplicationStateHeader
	if len(encoded) < ReplicationStateHeaderLength {
		return header, truncatedf("Replication State Header needs %d bytes, got %d", ReplicationStateHeaderLength, len(encoded))
	}
	if len(encoded) != ReplicationStateHeaderLength {
		return header, invalidf("Replication State Header has trailing bytes")
	}
	if !bytes.Equal(encoded[:8], replicationStateMagic[:]) {
		return header, invalidf("Replication State magic is invalid")
	}
	if version := getU16(encoded[8:10]); version != VersionV1 {
		return header, unsupportedVersion("Replication State", version)
	}
	if getU16(encoded[10:12]) != ReplicationStateHeaderLength {
		return header, invalidf("Replication State Header length is invalid")
	}
	flags := getU32(encoded[12:16])
	if flags&^replicationStateKnownFlags != 0 {
		return header, invalidf("Replication State flags contain unsupported bits: 0x%08x", flags)
	}
	for _, reserved := range []struct {
		data  []byte
		field string
	}{
		{encoded[124:128], "reserved_0"},
		{encoded[164:168], "reserved_1"},
		{encoded[180:184], "reserved_2"},
		{encoded[196:200], "reserved_3"},
		{encoded[212:216], "reserved_4"},
		{encoded[228:232], "reserved_5"},
		{encoded[268:272], "reserved_6"},
		{encoded[280:316], "reserved_7"},
	} {
		if err := expectZero(reserved.data, "Replication State "+reserved.field); err != nil {
			return header, err
		}
	}
	if stored, actual := getU32(encoded[316:320]), checksum(encoded[:316]); stored != actual {
		return header, checksumf("Replication State Header CRC32C is %08x, want %08x", stored, actual)
	}
	copy(header.StateID[:], encoded[16:32])
	header.Generation = getU64(encoded[32:40])
	header.PreviousGeneration = getU64(encoded[40:48])
	copy(header.PreviousStateSHA256[:], encoded[48:80])
	copy(header.GroupID[:], encoded[80:96])
	copy(header.NodeID[:], encoded[96:112])
	header.Term = getU64(encoded[112:120])
	header.Role = ReplicationRole(getU16(encoded[120:122]))
	header.Durability = ReplicationDurability(getU16(encoded[122:124]))
	header.HasLeader = flags&ReplicationStateHasLeader != 0
	copy(header.LeaderID[:], encoded[128:144])
	header.HasLease = flags&ReplicationStateHasLease != 0
	header.LeaseExpiresAt = getI64(encoded[144:152])
	header.LastAppended = getReplicationPosition(encoded[152:164], flags&ReplicationStateHasLastAppended != 0)
	header.LocalDurable = getReplicationPosition(encoded[168:180], flags&ReplicationStateHasLocalDurable != 0)
	header.Replicated = getReplicationPosition(encoded[184:196], flags&ReplicationStateHasReplicated != 0)
	header.Committed = getReplicationPosition(encoded[200:212], flags&ReplicationStateHasCommitted != 0)
	header.Applied = getReplicationPosition(encoded[216:228], flags&ReplicationStateHasApplied != 0)
	header.EarliestWALEntryID = getU64(encoded[232:240])
	header.HasInstalledSnapshot = flags&ReplicationStateHasInstalledSnapshot != 0
	copy(header.InstalledSnapshotID[:], encoded[240:256])
	header.InstalledSnapshotEntry = ReplicationPosition{Present: header.HasInstalledSnapshot, EntryID: getU64(encoded[256:264]), CRC32C: getU32(encoded[264:268])}
	header.CreatedAt = getI64(encoded[272:280])
	if err := validateReplicationStateHeader(header); err != nil {
		return ReplicationStateHeader{}, err
	}
	return header, nil
}

func MarshalReplicationState(state ReplicationState) ([]byte, error) {
	header, err := MarshalReplicationStateHeader(state.Header)
	if err != nil {
		return nil, err
	}
	footer, err := NewArtifactFooter(ArtifactReplicationState, state.Header.StateID, header)
	if err != nil {
		return nil, err
	}
	footerBytes, err := MarshalArtifactFooter(footer)
	if err != nil {
		return nil, err
	}
	return append(header, footerBytes...), nil
}

func UnmarshalReplicationState(encoded []byte) (ReplicationState, error) {
	if len(encoded) < ReplicationStateHeaderLength+ArtifactFooterLength {
		return ReplicationState{}, truncatedf("Replication State is truncated")
	}
	if len(encoded) != ReplicationStateHeaderLength+ArtifactFooterLength {
		return ReplicationState{}, invalidf("Replication State has unexpected bytes")
	}
	header, err := UnmarshalReplicationStateHeader(encoded[:ReplicationStateHeaderLength])
	if err != nil {
		return ReplicationState{}, err
	}
	footer, err := VerifyArtifact(encoded[:ReplicationStateHeaderLength], encoded[ReplicationStateHeaderLength:], ArtifactReplicationState, header.StateID)
	if err != nil {
		return ReplicationState{}, err
	}
	return ReplicationState{Header: header, Footer: footer}, nil
}

func MarshalReplicationCurrent(current ReplicationCurrent) ([]byte, error) {
	if err := validateReplicationCurrent(current); err != nil {
		return nil, err
	}
	length := replicationCurrentFixedLength + len(current.StateFileName)
	encoded := make([]byte, length)
	copy(encoded[:8], replicationCurrentMagic[:])
	putU16(encoded[8:10], VersionV1)
	putU16(encoded[10:12], uint16(length))
	putU64(encoded[16:24], current.Generation)
	copy(encoded[24:40], current.StateID[:])
	putU16(encoded[40:42], uint16(len(current.StateFileName)))
	copy(encoded[44:76], current.StateSHA256[:])
	crcPosition := 76 + copy(encoded[76:], current.StateFileName)
	putU32(encoded[crcPosition:], checksum(encoded[:crcPosition]))
	return encoded, nil
}

func UnmarshalReplicationCurrent(encoded []byte) (ReplicationCurrent, error) {
	var current ReplicationCurrent
	if len(encoded) < ReplicationCurrentMinLength {
		return current, truncatedf("REPLICATION-CURRENT needs at least %d bytes, got %d", ReplicationCurrentMinLength, len(encoded))
	}
	if !bytes.Equal(encoded[:8], replicationCurrentMagic[:]) {
		return current, invalidf("REPLICATION-CURRENT magic is invalid")
	}
	if version := getU16(encoded[8:10]); version != VersionV1 {
		return current, unsupportedVersion("REPLICATION-CURRENT", version)
	}
	if int(getU16(encoded[10:12])) != len(encoded) || getU32(encoded[12:16]) != 0 {
		return current, invalidf("REPLICATION-CURRENT length or flags are invalid")
	}
	if err := expectZero(encoded[42:44], "REPLICATION-CURRENT reserved"); err != nil {
		return current, err
	}
	nameLength := int(getU16(encoded[40:42]))
	if replicationCurrentFixedLength+nameLength != len(encoded) {
		return current, invalidf("REPLICATION-CURRENT State name length does not match file")
	}
	crcPosition := len(encoded) - 4
	if stored, actual := getU32(encoded[crcPosition:]), checksum(encoded[:crcPosition]); stored != actual {
		return current, checksumf("REPLICATION-CURRENT CRC32C is %08x, want %08x", stored, actual)
	}
	current.Generation = getU64(encoded[16:24])
	copy(current.StateID[:], encoded[24:40])
	copy(current.StateSHA256[:], encoded[44:76])
	current.StateFileName = string(bytes.Clone(encoded[76:crcPosition]))
	if err := validateReplicationCurrent(current); err != nil {
		return ReplicationCurrent{}, err
	}
	return current, nil
}

func validateReplicationStateHeader(header ReplicationStateHeader) error {
	if isZeroUUID(header.StateID) || isZeroUUID(header.GroupID) || isZeroUUID(header.NodeID) {
		return invalidf("Replication State identity is zero")
	}
	if header.Generation == 0 {
		if header.PreviousGeneration != 0 || !isZeroDigest(header.PreviousStateSHA256) {
			return invalidf("Replication State generation 0 has a previous State")
		}
	} else if header.PreviousGeneration != header.Generation-1 || isZeroDigest(header.PreviousStateSHA256) {
		return invalidf("Replication State does not continue the previous Generation")
	}
	positions := []struct {
		name     string
		position ReplicationPosition
	}{
		{"last_appended", header.LastAppended},
		{"local_durable", header.LocalDurable},
		{"committed", header.Committed},
		{"applied", header.Applied},
	}
	for _, value := range append(positions, struct {
		name     string
		position ReplicationPosition
	}{"replicated", header.Replicated}) {
		if !value.position.Present && (value.position.EntryID != 0 || value.position.CRC32C != 0) {
			return invalidf("Replication State %s has values without its Flag", value.name)
		}
	}
	for i := 1; i < len(positions); i++ {
		newer, older := positions[i-1], positions[i]
		if older.position.Present && (!newer.position.Present || older.position.EntryID > newer.position.EntryID) {
			return invalidf("Replication State %s exceeds %s", older.name, newer.name)
		}
		if older.position.Present && older.position.EntryID == newer.position.EntryID && older.position.CRC32C != newer.position.CRC32C {
			return invalidf("Replication State watermarks disagree at Entry %d", older.position.EntryID)
		}
	}
	if header.Replicated.Present && (!header.LastAppended.Present || header.Replicated.EntryID > header.LastAppended.EntryID) {
		return invalidf("Replication State replicated exceeds last_appended")
	}
	if header.Replicated.Present && header.LastAppended.Present && header.Replicated.EntryID == header.LastAppended.EntryID && header.Replicated.CRC32C != header.LastAppended.CRC32C {
		return invalidf("Replication State replicated checksum disagrees with last_appended")
	}
	if !header.LastAppended.Present {
		if header.EarliestWALEntryID != 0 {
			return invalidf("empty Replication State has nonzero earliest WAL Entry")
		}
	} else if header.LastAppended.EntryID != math.MaxUint64 && header.EarliestWALEntryID > header.LastAppended.EntryID+1 {
		return invalidf("Replication State earliest WAL Entry is beyond the log tail")
	}
	if err := validateReplicationRole(header); err != nil {
		return err
	}
	if err := validateReplicationSnapshot(header); err != nil {
		return err
	}
	return nil
}

func validateReplicationRole(header ReplicationStateHeader) error {
	if !header.HasLeader && !isZeroUUID(header.LeaderID) {
		return invalidf("Replication State has Leader ID without Flag")
	}
	if header.HasLeader && isZeroUUID(header.LeaderID) {
		return invalidf("Replication State Leader ID is zero")
	}
	if !header.HasLease && header.LeaseExpiresAt != 0 {
		return invalidf("Replication State has Lease deadline without Flag")
	}
	if header.HasLease && header.LeaseExpiresAt <= 0 {
		return invalidf("Replication State Lease deadline is invalid")
	}
	switch header.Role {
	case ReplicationRoleSingle:
		if header.Durability != ReplicationDurabilitySingleSync || header.HasLeader || header.HasLease || header.Replicated.Present {
			return invalidf("SINGLE Replication State contains replicated fields")
		}
	case ReplicationRolePrimary:
		if (header.Durability != ReplicationDurabilityStrict && header.Durability != ReplicationDurabilityDegraded) || !header.HasLeader || header.LeaderID != header.NodeID || !header.HasLease {
			return invalidf("PRIMARY Replication State role fields are invalid")
		}
		if header.Durability == ReplicationDurabilityStrict && header.Committed.Present && (!header.Replicated.Present || header.Committed.EntryID > header.Replicated.EntryID) {
			return invalidf("Strict PRIMARY committed exceeds replicated")
		}
	case ReplicationRoleStandby:
		if header.Durability != ReplicationDurabilityStrict || !header.HasLeader || header.LeaderID == header.NodeID || header.HasLease || header.Replicated.Present {
			return invalidf("STANDBY Replication State role fields are invalid")
		}
	case ReplicationRoleRecovering:
		if header.Durability != ReplicationDurabilityStrict || header.HasLease || header.Replicated.Present || (header.HasLeader && header.LeaderID == header.NodeID) {
			return invalidf("RECOVERING Replication State role fields are invalid")
		}
	default:
		return invalidf("Replication State Role %d is invalid", header.Role)
	}
	return nil
}

func validateReplicationSnapshot(header ReplicationStateHeader) error {
	if !header.HasInstalledSnapshot {
		if !isZeroUUID(header.InstalledSnapshotID) || header.InstalledSnapshotEntry.Present || header.InstalledSnapshotEntry.EntryID != 0 || header.InstalledSnapshotEntry.CRC32C != 0 {
			return invalidf("Replication State has Snapshot values without Flag")
		}
		if header.EarliestWALEntryID > 0 {
			return invalidf("Replication State collected WAL without an installed Snapshot")
		}
		return nil
	}
	checkpoint := header.InstalledSnapshotEntry
	if isZeroUUID(header.InstalledSnapshotID) || !checkpoint.Present || !header.Committed.Present || checkpoint.EntryID > header.Committed.EntryID {
		return invalidf("Replication State installed Snapshot is not covered by Commit")
	}
	if checkpoint.EntryID == header.Committed.EntryID && checkpoint.CRC32C != header.Committed.CRC32C {
		return invalidf("Replication State Snapshot checksum disagrees with Commit")
	}
	if header.EarliestWALEntryID > 0 && checkpoint.EntryID < header.EarliestWALEntryID-1 {
		return invalidf("Replication State Snapshot does not cover collected WAL")
	}
	return nil
}

func replicationStateFlags(header ReplicationStateHeader) uint32 {
	var flags uint32
	for _, value := range []struct {
		present bool
		flag    uint32
	}{
		{header.LastAppended.Present, ReplicationStateHasLastAppended},
		{header.LocalDurable.Present, ReplicationStateHasLocalDurable},
		{header.Replicated.Present, ReplicationStateHasReplicated},
		{header.Committed.Present, ReplicationStateHasCommitted},
		{header.Applied.Present, ReplicationStateHasApplied},
		{header.HasLeader, ReplicationStateHasLeader},
		{header.HasLease, ReplicationStateHasLease},
		{header.HasInstalledSnapshot, ReplicationStateHasInstalledSnapshot},
	} {
		if value.present {
			flags |= value.flag
		}
	}
	return flags
}

func putReplicationPosition(encoded []byte, position ReplicationPosition) {
	putU64(encoded[:8], position.EntryID)
	putU32(encoded[8:12], position.CRC32C)
}

func getReplicationPosition(encoded []byte, present bool) ReplicationPosition {
	return ReplicationPosition{Present: present, EntryID: getU64(encoded[:8]), CRC32C: getU32(encoded[8:12])}
}

func validateReplicationCurrent(current ReplicationCurrent) error {
	if isZeroUUID(current.StateID) || isZeroDigest(current.StateSHA256) {
		return invalidf("REPLICATION-CURRENT has zero State identity or SHA-256")
	}
	name := current.StateFileName
	if name == "" || !utf8.ValidString(name) || strings.ContainsRune(name, 0) || strings.ContainsAny(name, "/\\") || name == "." || name == ".." {
		return invalidf("REPLICATION-CURRENT State name is not one valid filename")
	}
	if replicationCurrentFixedLength+len(name) > int(^uint16(0)) || len(name) > int(^uint16(0)) {
		return fmtTooLarge("REPLICATION-CURRENT", replicationCurrentFixedLength+len(name), ^uint16(0))
	}
	return nil
}

func ReplicationStateFileName(generation uint64, stateID UUID) string {
	return fmt.Sprintf("REPLICATION-STATE-%020d-%x.bin", generation, stateID)
}
