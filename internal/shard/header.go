package shard

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// Magic is the 4-byte header identifier "SVRD" (ShardVault Reed-solomon Data)
var Magic = [4]byte{'S', 'V', 'R', 'D'}

const (
	CurrentVersion = 1
	// HeaderSize is:
	// 4 (Magic) + 2 (Version) + 2 (Reserved) + 26 (ULID string) +
	// 4 (StripeIdx uint32) + 2 (ShardIdx uint16) + 2 (Reserved) +
	// 8 (DataLen uint64) + 4 (DataCRC32C uint32) = 54 bytes
	HeaderSize = 54
)

var (
	ErrInvalidMagic   = errors.New("shard: invalid magic bytes")
	ErrInvalidVersion = errors.New("shard: unsupported header version")
	ErrCorruptData    = errors.New("shard: data crc32c mismatch (corrupt)")
	ErrHeaderTooShort = errors.New("shard: header buffer too short")
)

// ShardHeader encodes metadata at the beginning of each shard file on storage nodes.
type ShardHeader struct {
	Version         uint16 // Format version (1)
	ObjectVersionID string // 26-char ULID
	StripeIdx       uint32 // 0-indexed stripe number
	ShardIdx        uint16 // 0..k+m-1
	DataLen         uint64 // payload length in bytes
	DataCRC32C      uint32 // CRC32-Castagnoli checksum of the payload
}

// Castagnoli table used across ShardVault
var CastagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// Checksum computes the CRC32C of data.
func Checksum(data []byte) uint32 {
	return crc32.Checksum(data, CastagnoliTable)
}

// Encode serializes the ShardHeader into a 54-byte slice.
func (h *ShardHeader) Encode() []byte {
	buf := make([]byte, HeaderSize)
	copy(buf[0:4], Magic[:])
	binary.BigEndian.PutUint16(buf[4:6], h.Version)
	binary.BigEndian.PutUint16(buf[6:8], 0) // reserved

	// ULID string: exactly 26 bytes. If shorter, pad with space; if longer, truncate
	var ulidBuf [26]byte
	copy(ulidBuf[:], h.ObjectVersionID)
	copy(buf[8:34], ulidBuf[:])

	binary.BigEndian.PutUint32(buf[34:38], h.StripeIdx)
	binary.BigEndian.PutUint16(buf[38:40], h.ShardIdx)
	binary.BigEndian.PutUint16(buf[40:42], 0) // reserved
	binary.BigEndian.PutUint64(buf[42:50], h.DataLen)
	binary.BigEndian.PutUint32(buf[50:54], h.DataCRC32C)
	return buf
}

// Decode parses a ShardHeader from bytes.
func DecodeHeader(buf []byte) (*ShardHeader, error) {
	if len(buf) < HeaderSize {
		return nil, ErrHeaderTooShort
	}
	if buf[0] != Magic[0] || buf[1] != Magic[1] || buf[2] != Magic[2] || buf[3] != Magic[3] {
		return nil, ErrInvalidMagic
	}
	ver := binary.BigEndian.Uint16(buf[4:6])
	if ver != CurrentVersion {
		return nil, fmt.Errorf("%w: got %d, expected %d", ErrInvalidVersion, ver, CurrentVersion)
	}

	ulidStr := string(buf[8:34])
	stripeIdx := binary.BigEndian.Uint32(buf[34:38])
	shardIdx := binary.BigEndian.Uint16(buf[38:40])
	dataLen := binary.BigEndian.Uint64(buf[42:50])
	dataCRC := binary.BigEndian.Uint32(buf[50:54])

	return &ShardHeader{
		Version:         ver,
		ObjectVersionID: ulidStr,
		StripeIdx:       stripeIdx,
		ShardIdx:        shardIdx,
		DataLen:         dataLen,
		DataCRC32C:      dataCRC,
	}, nil
}

// ReadHeader reads exactly HeaderSize bytes from r and decodes the header.
func ReadHeader(r io.Reader) (*ShardHeader, error) {
	buf := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return DecodeHeader(buf)
}
