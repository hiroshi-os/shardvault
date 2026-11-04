package ec

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/klauspost/reedsolomon"
)

var (
	ErrTooFewShards  = errors.New("ec: fewer than k shards available")
	ErrShardsCorrupt = errors.New("ec: stripe SHA-256 verification failed")
)

// Codec handles Reed-Solomon encoding and reconstruction.
type Codec struct {
	k   int
	m   int
	enc reedsolomon.Encoder
}

// NewCodec creates a new Reed-Solomon codec for k data shards and m parity shards.
func NewCodec(k, m int) (*Codec, error) {
	enc, err := reedsolomon.New(k, m)
	if err != nil {
		return nil, fmt.Errorf("failed to create reedsolomon encoder: %w", err)
	}
	return &Codec{
		k:   k,
		m:   m,
		enc: enc,
	}, nil
}

// K returns the number of data shards.
func (c *Codec) K() int { return c.k }

// M returns the number of parity shards.
func (c *Codec) M() int { return c.m }

// Total returns total shards (k + m).
func (c *Codec) Total() int { return c.k + c.m }

// EncodeStripe splits data into k equal parts (padded with 0s if needed) and generates m parity shards.
// It returns:
// - shards: [][]byte of length k+m, each shard having the same length
// - origLen: the unpadded length of data
// - stripeSHA: sha256 checksum of the original unpadded data
func (c *Codec) EncodeStripe(data []byte) (shards [][]byte, origLen int, stripeSHA [32]byte, err error) {
	origLen = len(data)
	stripeSHA = sha256.Sum256(data)

	// Shard size is ceil(origLen / k)
	shardSize := (origLen + c.k - 1) / c.k
	if shardSize == 0 {
		shardSize = 1 // avoid 0-byte allocations
	}

	shards = make([][]byte, c.k+c.m)
	for i := 0; i < c.k; i++ {
		shards[i] = make([]byte, shardSize)
		start := i * shardSize
		if start < origLen {
			end := start + shardSize
			if end > origLen {
				end = origLen
			}
			copy(shards[i], data[start:end])
		}
	}
	for i := c.k; i < c.k+c.m; i++ {
		shards[i] = make([]byte, shardSize)
	}

	if err := c.enc.Encode(shards); err != nil {
		return nil, 0, [32]byte{}, fmt.Errorf("encode failed: %w", err)
	}

	return shards, origLen, stripeSHA, nil
}

// Reconstruct recovers missing or corrupted shards given available shards.
// availableShards must have length k+m, with missing/corrupted shards set to nil or empty byte slices.
// If successful, all missing shards in availableShards are filled.
func (c *Codec) Reconstruct(availableShards [][]byte) error {
	if len(availableShards) != c.k+c.m {
		return fmt.Errorf("invalid shard count: expected %d, got %d", c.k+c.m, len(availableShards))
	}
	present := 0
	for i := range availableShards {
		if len(availableShards[i]) > 0 {
			present++
		} else {
			availableShards[i] = nil
		}
	}
	if present < c.k {
		return ErrTooFewShards
	}

	return c.enc.Reconstruct(availableShards)
}

// DecodeStripe decodes the original data from shards, trimming padding according to origLen,
// and verifies the reconstructed data against expectedSHA.
func (c *Codec) DecodeStripe(shards [][]byte, origLen int, expectedSHA [32]byte) ([]byte, error) {
	if err := c.Reconstruct(shards); err != nil {
		return nil, err
	}

	// Reconstruct data from k data shards
	buf := make([]byte, 0, origLen)
	remaining := origLen
	for i := 0; i < c.k; i++ {
		if remaining <= 0 {
			break
		}
		take := len(shards[i])
		if take > remaining {
			take = remaining
		}
		buf = append(buf, shards[i][:take]...)
		remaining -= take
	}

	actualSHA := sha256.Sum256(buf)
	if actualSHA != expectedSHA {
		return nil, ErrShardsCorrupt
	}

	return buf, nil
}
