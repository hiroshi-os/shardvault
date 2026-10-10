package stripe

import (
	"testing"
)

func TestRangeBoundaryMath(t *testing.T) {
	stripeSize := int64(1048576) // 1 MiB

	// Zero-length object
	span0, err := CalculateSpan(0, stripeSize, 0, 0)
	if err != nil {
		t.Fatalf("CalculateSpan on 0-len object failed: %v", err)
	}
	if span0.StartStripe != 0 || span0.EndStripe != 0 || span0.TotalStripes != 1 {
		t.Errorf("unexpected span for 0-len: %+v", span0)
	}

	// 1-byte object
	span1, err := CalculateSpan(1, stripeSize, 0, 0)
	if err != nil {
		t.Fatalf("CalculateSpan 1-byte failed: %v", err)
	}
	if span1.StartStripe != 0 || span1.EndStripe != 0 || span1.TotalStripes != 1 {
		t.Errorf("unexpected span for 1-byte: %+v", span1)
	}

	// 1 MiB object (exactly 1 stripe)
	span1M, err := CalculateSpan(stripeSize, stripeSize, 0, stripeSize-1)
	if err != nil {
		t.Fatalf("CalculateSpan 1MiB failed: %v", err)
	}
	if span1M.StartStripe != 0 || span1M.EndStripe != 0 || span1M.TotalStripes != 1 {
		t.Errorf("unexpected span for 1MiB: %+v", span1M)
	}

	// 2.5 MiB object (3 stripes: [0, 1M), [1M, 2M), [2M, 2.5M))
	totalSize := int64(2.5 * 1024 * 1024)
	totalStripes := CalculateTotalStripes(totalSize, stripeSize)
	if totalStripes != 3 {
		t.Fatalf("expected 3 stripes, got %d", totalStripes)
	}

	// First byte (0-0)
	firstByte, err := CalculateSpan(totalSize, stripeSize, 0, 0)
	if err != nil || firstByte.StartStripe != 0 || firstByte.EndStripe != 0 {
		t.Errorf("first byte span mismatch: %+v, err: %v", firstByte, err)
	}

	// Last byte (totalSize-1)
	lastByte, err := CalculateSpan(totalSize, stripeSize, totalSize-1, totalSize-1)
	if err != nil || lastByte.StartStripe != 2 || lastByte.EndStripe != 2 {
		t.Errorf("last byte span mismatch: %+v, err: %v", lastByte, err)
	}

	// Span across stripes: from 500 KiB to 1.5 MiB (stripes 0 and 1)
	crossSpan, err := CalculateSpan(totalSize, stripeSize, 500*1024, int64(1.5*1024*1024))
	if err != nil || crossSpan.StartStripe != 0 || crossSpan.EndStripe != 1 {
		t.Errorf("cross span mismatch: %+v, err: %v", crossSpan, err)
	}

	// Invalid ranges: negative start, end < start, end >= size
	if _, err := CalculateSpan(totalSize, stripeSize, -1, 100); err == nil {
		t.Errorf("expected error for negative start")
	}
	if _, err := CalculateSpan(totalSize, stripeSize, 100, 50); err == nil {
		t.Errorf("expected error for end < start")
	}
	if _, err := CalculateSpan(totalSize, stripeSize, 0, totalSize); err == nil {
		t.Errorf("expected error for end >= size")
	}
}

func TestStripeByteBounds(t *testing.T) {
	stripeSize := int64(100)

	// Object range [50, 150] spanning stripe 0 and stripe 1
	// For stripe 0 [0..99]: bounds should be [50, 100)
	s0Start, s0End := StripeByteBounds(0, stripeSize, 50, 150)
	if s0Start != 50 || s0End != 100 {
		t.Errorf("stripe 0 bounds got [%d, %d), expected [50, 100)", s0Start, s0End)
	}

	// For stripe 1 [100..199]: bounds should be [0, 51) (since 150 - 100 + 1 = 51 bytes)
	s1Start, s1End := StripeByteBounds(1, stripeSize, 50, 150)
	if s1Start != 0 || s1End != 51 {
		t.Errorf("stripe 1 bounds got [%d, %d), expected [0, 51)", s1Start, s1End)
	}
}
