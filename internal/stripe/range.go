package stripe

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidRange = errors.New("stripe: invalid byte range")
)

// RangeSpan calculates which stripes are required for a byte range [start, end] (inclusive)
// within an object of totalSize bytes, given a stripeSize.
// Note: In HTTP Range header, "bytes=start-end" is inclusive of end.
type StripeSpan struct {
	StartStripe  uint32
	EndStripe    uint32
	TotalStripes uint32
}

// CalculateTotalStripes returns the total number of stripes for an object of given size.
// A 0-byte object has 1 stripe (an empty stripe).
func CalculateTotalStripes(objectSize int64, stripeSize int64) uint32 {
	if objectSize <= 0 {
		return 1
	}
	stripes := objectSize / stripeSize
	if objectSize%stripeSize != 0 {
		stripes++
	}
	return uint32(stripes)
}

// CalculateSpan returns the range of stripes (inclusive) needed to read bytes [start, end].
func CalculateSpan(objectSize int64, stripeSize int64, start, end int64) (StripeSpan, error) {
	if objectSize < 0 || stripeSize <= 0 {
		return StripeSpan{}, errors.New("stripe: invalid objectSize or stripeSize")
	}

	totalStripes := CalculateTotalStripes(objectSize, stripeSize)

	if objectSize == 0 {
		if start == 0 && end == 0 {
			return StripeSpan{StartStripe: 0, EndStripe: 0, TotalStripes: 1}, nil
		}
		return StripeSpan{}, ErrInvalidRange
	}

	if start < 0 || end < start || end >= objectSize {
		return StripeSpan{}, fmt.Errorf("%w: [%d, %d] outside [0, %d)", ErrInvalidRange, start, end, objectSize)
	}

	startStripe := uint32(start / stripeSize)
	endStripe := uint32(end / stripeSize)

	return StripeSpan{
		StartStripe:  startStripe,
		EndStripe:    endStripe,
		TotalStripes: totalStripes,
	}, nil
}

// StripeByteBounds returns the slice of bytes [inStripeStart, inStripeEnd) within a given stripe
// that corresponds to the requested overall object range [reqStart, reqEnd] (inclusive).
func StripeByteBounds(stripeIdx uint32, stripeSize int64, reqStart, reqEnd int64) (int64, int64) {
	stripeStartByte := int64(stripeIdx) * stripeSize

	// Overlap between stripe and [reqStart, reqEnd]
	relStart := reqStart - stripeStartByte
	if relStart < 0 {
		relStart = 0
	}

	relEnd := reqEnd - stripeStartByte + 1 // exclusive length
	if relEnd > stripeSize {
		relEnd = stripeSize
	}

	return relStart, relEnd
}
