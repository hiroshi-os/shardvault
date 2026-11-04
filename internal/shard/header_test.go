package shard

import (
	"bytes"
	"testing"
)

func TestShardHeaderRoundTrip(t *testing.T) {
	orig := &ShardHeader{
		Version:         CurrentVersion,
		ObjectVersionID: "01J9XYZ0123456789ABCDEF012",
		StripeIdx:       42,
		ShardIdx:        3,
		DataLen:         262144,
		DataCRC32C:      123456789,
	}

	encoded := orig.Encode()
	if len(encoded) != HeaderSize {
		t.Fatalf("expected header size %d, got %d", HeaderSize, len(encoded))
	}

	decoded, err := DecodeHeader(encoded)
	if err != nil {
		t.Fatalf("DecodeHeader failed: %v", err)
	}

	if decoded.Version != orig.Version {
		t.Errorf("version mismatch: got %d, want %d", decoded.Version, orig.Version)
	}
	if decoded.ObjectVersionID != orig.ObjectVersionID {
		t.Errorf("versionID mismatch: got %q, want %q", decoded.ObjectVersionID, orig.ObjectVersionID)
	}
	if decoded.StripeIdx != orig.StripeIdx {
		t.Errorf("stripeIdx mismatch: got %d, want %d", decoded.StripeIdx, orig.StripeIdx)
	}
	if decoded.ShardIdx != orig.ShardIdx {
		t.Errorf("shardIdx mismatch: got %d, want %d", decoded.ShardIdx, orig.ShardIdx)
	}
	if decoded.DataLen != orig.DataLen {
		t.Errorf("dataLen mismatch: got %d, want %d", decoded.DataLen, orig.DataLen)
	}
	if decoded.DataCRC32C != orig.DataCRC32C {
		t.Errorf("dataCRC32C mismatch: got %d, want %d", decoded.DataCRC32C, orig.DataCRC32C)
	}
}

func TestShardHeaderErrors(t *testing.T) {
	// Too short
	short := make([]byte, 10)
	if _, err := DecodeHeader(short); err != ErrHeaderTooShort {
		t.Fatalf("expected ErrHeaderTooShort, got %v", err)
	}

	// Bad magic
	badMagic := (&ShardHeader{Version: 1}).Encode()
	badMagic[0] = 'X'
	if _, err := DecodeHeader(badMagic); err != ErrInvalidMagic {
		t.Fatalf("expected ErrInvalidMagic, got %v", err)
	}

	// Bad version
	badVer := (&ShardHeader{Version: 99}).Encode()
	badVer[4] = 0
	badVer[5] = 99
	if _, err := DecodeHeader(badVer); err == nil {
		t.Fatalf("expected version error, got nil")
	}
}

func FuzzShardHeader(f *testing.F) {
	// Seed with a valid header
	valid := (&ShardHeader{
		Version:         CurrentVersion,
		ObjectVersionID: "01J9XYZ0123456789ABCDEF012",
		StripeIdx:       10,
		ShardIdx:        2,
		DataLen:         1024,
		DataCRC32C:      Checksum([]byte("test")),
	}).Encode()
	f.Add(valid)
	f.Add([]byte("too short"))
	f.Add(make([]byte, HeaderSize))

	f.Fuzz(func(t *testing.T, data []byte) {
		h, err := DecodeHeader(data)
		if err != nil {
			return
		}
		// If decode succeeded, re-encoding must succeed and be deterministically re-decodable
		encoded := h.Encode()
		h2, err2 := DecodeHeader(encoded)
		if err2 != nil {
			t.Fatalf("re-decode failed after successful decode: %v", err2)
		}
		if !bytes.Equal(encoded, h2.Encode()) {
			t.Fatalf("re-encode mismatch")
		}
	})
}
