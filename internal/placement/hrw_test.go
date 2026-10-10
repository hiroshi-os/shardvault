package placement

import (
	"fmt"
	"math"
	"testing"
)

func TestHRWDistinctNodes(t *testing.T) {
	nodes := []string{"node1", "node2", "node3", "node4", "node5", "node6"}
	hrw, err := NewHRW(nodes)
	if err != nil {
		t.Fatalf("NewHRW failed: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("01J9XYZ_%d", i)
		picked, err := hrw.PickNodes(key, 6)
		if err != nil {
			t.Fatalf("PickNodes failed: %v", err)
		}
		if len(picked) != 6 {
			t.Fatalf("expected 6 nodes, got %d", len(picked))
		}
		seen := make(map[string]bool)
		for _, n := range picked {
			if seen[n] {
				t.Fatalf("duplicate node %s returned for key %s", n, key)
			}
			seen[n] = true
		}
	}
}

func TestHRWStabilityUnderNodeAddition(t *testing.T) {
	initialNodes := []string{"node1", "node2", "node3", "node4", "node5", "node6"}
	hrw6, _ := NewHRW(initialNodes)

	expandedNodes := []string{"node1", "node2", "node3", "node4", "node5", "node6", "node7"}
	hrw7, _ := NewHRW(expandedNodes)

	keysCount := 1000
	movedCount := 0
	for i := 0; i < keysCount; i++ {
		key := fmt.Sprintf("stripe_%d", i)
		p6, _ := hrw6.PickNodes(key, 1)
		p7, _ := hrw7.PickNodes(key, 1)

		if p6[0] != p7[0] {
			movedCount++
			// When adding node7, any moved key MUST move to node7, not to any other existing node!
			if p7[0] != "node7" {
				t.Fatalf("key %s moved from %s to %s instead of node7", key, p6[0], p7[0])
			}
		}
	}

	fractionMoved := float64(movedCount) / float64(keysCount)
	expectedFraction := 1.0 / 7.0 // ~0.1428
	t.Logf("Measured fraction moved when adding 1 node to 6: %.4f (theoretical 1/7 = %.4f)", fractionMoved, expectedFraction)
	if math.Abs(fractionMoved-expectedFraction) > 0.05 {
		t.Errorf("moved fraction %.4f diverged too much from theoretical %.4f", fractionMoved, expectedFraction)
	}
}

func TestHRWUniformDistribution(t *testing.T) {
	nodes := []string{"node1", "node2", "node3", "node4", "node5", "node6"}
	hrw, _ := NewHRW(nodes)

	counts := make(map[string]int)
	totalKeys := 60000
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		picked, _ := hrw.PickNodes(key, 1)
		counts[picked[0]]++
	}

	expectedPerNode := float64(totalKeys) / float64(len(nodes)) // 10000
	for node, count := range counts {
		diffRatio := math.Abs(float64(count)-expectedPerNode) / expectedPerNode
		t.Logf("Node %s: %d keys (diff: %.2f%%)", node, count, diffRatio*100)
		if diffRatio > 0.10 { // within 10% tolerance for 60k keys
			t.Errorf("node %s count %d deviates too much from expected %.0f (%.2f%%)", node, count, expectedPerNode, diffRatio*100)
		}
	}
}
