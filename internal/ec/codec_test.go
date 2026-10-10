package ec

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestErasureRoundTripAllCombinations(t *testing.T) {
	codec, err := NewCodec(4, 2)
	if err != nil {
		t.Fatalf("failed to create codec: %v", err)
	}

	testSizes := []int{0, 1, 10, 100, 1024, 65536, 1048576, 1048576 + 333}

	for _, size := range testSizes {
		data := make([]byte, size)
		if size > 0 {
			if _, err := rand.Read(data); err != nil {
				t.Fatalf("rand.Read failed: %v", err)
			}
		}

		shards, origLen, stripeSHA, err := codec.EncodeStripe(data)
		if err != nil {
			t.Fatalf("EncodeStripe size %d failed: %v", size, err)
		}
		if len(shards) != 6 {
			t.Fatalf("expected 6 shards, got %d", len(shards))
		}

		// 1. Zero lost shards
		testShards := make([][]byte, 6)
		for i := 0; i < 6; i++ {
			testShards[i] = append([]byte(nil), shards[i]...)
		}
		decoded, err := codec.DecodeStripe(testShards, origLen, stripeSHA)
		if err != nil {
			t.Fatalf("decode 0 lost failed for size %d: %v", size, err)
		}
		if !bytes.Equal(decoded, data) {
			t.Fatalf("decoded bytes mismatch for 0 lost, size %d", size)
		}

		// 2. All 6 single-shard loss combinations
		for i := 0; i < 6; i++ {
			testShards := make([][]byte, 6)
			for j := 0; j < 6; j++ {
				if j != i {
					testShards[j] = append([]byte(nil), shards[j]...)
				}
			}
			decoded, err := codec.DecodeStripe(testShards, origLen, stripeSHA)
			if err != nil {
				t.Fatalf("decode 1 lost (missing %d) failed for size %d: %v", i, size, err)
			}
			if !bytes.Equal(decoded, data) {
				t.Fatalf("decoded bytes mismatch for 1 lost (missing %d), size %d", i, size)
			}
		}

		// 3. All 15 two-shard loss combinations (m=2)
		combCount := 0
		for i := 0; i < 6; i++ {
			for j := i + 1; j < 6; j++ {
				combCount++
				testShards := make([][]byte, 6)
				for k := 0; k < 6; k++ {
					if k != i && k != j {
						testShards[k] = append([]byte(nil), shards[k]...)
					}
				}
				decoded, err := codec.DecodeStripe(testShards, origLen, stripeSHA)
				if err != nil {
					t.Fatalf("decode 2 lost (missing %d, %d) failed for size %d: %v", i, j, size, err)
				}
				if !bytes.Equal(decoded, data) {
					t.Fatalf("decoded bytes mismatch for 2 lost (missing %d, %d), size %d", i, j, size)
				}
			}
		}
		if combCount != 15 {
			t.Fatalf("expected 15 combinations, got %d", combCount)
		}

		// 4. Three shards lost (more than m=2) must fail
		badShards := make([][]byte, 6)
		badShards[0] = append([]byte(nil), shards[0]...)
		badShards[1] = append([]byte(nil), shards[1]...)
		badShards[2] = append([]byte(nil), shards[2]...)
		// shards 3, 4, 5 are nil (only 3 healthy shards, need 4)
		_, err = codec.DecodeStripe(badShards, origLen, stripeSHA)
		if err == nil {
			t.Fatalf("expected error when 3 shards missing, got nil")
		}
	}
}

func BenchmarkEncode(b *testing.B) {
	codec, err := NewCodec(4, 2)
	if err != nil {
		b.Fatalf("NewCodec failed: %v", err)
	}
	size := 1024 * 1024 // 1 MiB stripe
	data := make([]byte, size)
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, err := codec.EncodeStripe(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReconstruct(b *testing.B) {
	codec, err := NewCodec(4, 2)
	if err != nil {
		b.Fatalf("NewCodec failed: %v", err)
	}
	size := 1024 * 1024 // 1 MiB stripe
	data := make([]byte, size)
	shards, _, _, err := codec.EncodeStripe(data)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testShards := make([][]byte, 6)
		testShards[0] = append([]byte(nil), shards[0]...)
		testShards[1] = append([]byte(nil), shards[1]...)
		// 2 and 3 missing
		testShards[4] = append([]byte(nil), shards[4]...)
		testShards[5] = append([]byte(nil), shards[5]...)
		if err := codec.Reconstruct(testShards); err != nil {
			b.Fatal(err)
		}
	}
}
