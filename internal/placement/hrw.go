package placement

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

var (
	ErrNotEnoughNodes = errors.New("placement: not enough nodes configured for k+m")
)

// HRW represents Rendezvous (Highest Random Weight) hashing over a list of storage nodes.
type HRW struct {
	nodes []string
}

// NewHRW creates a new HRW ring with the given storage nodes.
func NewHRW(nodes []string) (*HRW, error) {
	if len(nodes) == 0 {
		return nil, errors.New("placement: node list cannot be empty")
	}
	// copy and sort nodes for determinism
	ns := make([]string, len(nodes))
	copy(ns, nodes)
	sort.Strings(ns)
	return &HRW{nodes: ns}, nil
}

type scoredNode struct {
	node  string
	score uint64
}

// hash64 computes a 64-bit cryptographic hash of node + "/" + key using SHA-256 for optimal distribution.
func hash64(node, key string) uint64 {
	h := sha256.New()
	h.Write([]byte(key))
	h.Write([]byte("/"))
	h.Write([]byte(node))
	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}

// PickNodes returns top count distinct nodes for the given key (e.g. "versionID/stripeIdx").
// Higher score wins. Ties are broken lexicographically by node name.
func (h *HRW) PickNodes(key string, count int) ([]string, error) {
	if count > len(h.nodes) {
		return nil, fmt.Errorf("%w: requested %d, available %d", ErrNotEnoughNodes, count, len(h.nodes))
	}

	scored := make([]scoredNode, len(h.nodes))
	for i, node := range h.nodes {
		scored[i] = scoredNode{
			node:  node,
			score: hash64(node, key),
		}
	}

	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score // descending
		}
		return scored[i].node < scored[j].node
	})

	res := make([]string, count)
	for i := 0; i < count; i++ {
		res[i] = scored[i].node
	}
	return res, nil
}

// Nodes returns the full configured list of nodes.
func (h *HRW) Nodes() []string {
	out := make([]string, len(h.nodes))
	copy(out, h.nodes)
	return out
}
