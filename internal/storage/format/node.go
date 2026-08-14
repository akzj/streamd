package format

import "bytes"

var nodeIdentityMagic = [8]byte{'S', 'T', 'R', 'M', 'N', 'O', 'D', 'E'}

type NodeIdentity struct {
	ClusterID UUID
	GroupID   UUID
	NodeID    UUID
	CreatedAt int64
}

func MarshalNodeIdentity(node NodeIdentity) ([]byte, error) {
	if err := validateNodeIdentity(node); err != nil {
		return nil, err
	}
	b := make([]byte, NodeIdentityLength)
	copy(b[:8], nodeIdentityMagic[:])
	putU16(b[8:10], VersionV1)
	putU16(b[10:12], NodeIdentityLength)
	copy(b[16:32], node.ClusterID[:])
	copy(b[32:48], node.GroupID[:])
	copy(b[48:64], node.NodeID[:])
	putI64(b[64:72], node.CreatedAt)
	putU32(b[76:80], checksum(b[:76]))
	return b, nil
}

func UnmarshalNodeIdentity(b []byte) (NodeIdentity, error) {
	var node NodeIdentity
	if len(b) < NodeIdentityLength {
		return node, truncatedf("NODE needs %d bytes, got %d", NodeIdentityLength, len(b))
	}
	if len(b) != NodeIdentityLength {
		return node, invalidf("NODE has trailing bytes")
	}
	if !bytes.Equal(b[:8], nodeIdentityMagic[:]) {
		return node, invalidf("NODE magic is invalid")
	}
	if v := getU16(b[8:10]); v != VersionV1 {
		return node, unsupportedVersion("NODE", v)
	}
	if getU16(b[10:12]) != NodeIdentityLength || getU32(b[12:16]) != 0 {
		return node, invalidf("NODE length or flags are invalid")
	}
	if err := expectZero(b[72:76], "NODE reserved"); err != nil {
		return node, err
	}
	if stored, actual := getU32(b[76:80]), checksum(b[:76]); stored != actual {
		return node, checksumf("NODE CRC32C is %08x, want %08x", stored, actual)
	}
	copy(node.ClusterID[:], b[16:32])
	copy(node.GroupID[:], b[32:48])
	copy(node.NodeID[:], b[48:64])
	node.CreatedAt = getI64(b[64:72])
	if err := validateNodeIdentity(node); err != nil {
		return NodeIdentity{}, err
	}
	return node, nil
}

func validateNodeIdentity(node NodeIdentity) error {
	if isZeroUUID(node.ClusterID) || isZeroUUID(node.GroupID) || isZeroUUID(node.NodeID) {
		return invalidf("NODE identity contains a zero UUID")
	}
	if node.ClusterID == node.GroupID || node.ClusterID == node.NodeID || node.GroupID == node.NodeID {
		return invalidf("NODE UUIDs must be distinct")
	}
	return nil
}
