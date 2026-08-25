package hashring

import "testing"

func TestNodesForReturnsRequestedReplicaCount(t *testing.T) {
	r := New()
	r.AddNode("a")
	r.AddNode("b")
	r.AddNode("c")

	nodes := r.NodesFor("some-key", 2)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d: %v", len(nodes), nodes)
	}
	if nodes[0] == nodes[1] {
		t.Fatalf("replica set should not contain duplicates: %v", nodes)
	}
}

func TestNodesForIsDeterministic(t *testing.T) {
	r := New()
	r.AddNode("a")
	r.AddNode("b")
	r.AddNode("c")

	first := r.NodesFor("stable-key", 2)
	second := r.NodesFor("stable-key", 2)
	if first[0] != second[0] || first[1] != second[1] {
		t.Fatalf("expected same key to map to same nodes: %v vs %v", first, second)
	}
}

func TestRemoveNodeFallsThroughToRemainingNodes(t *testing.T) {
	r := New()
	r.AddNode("a")
	r.AddNode("b")
	r.AddNode("c")

	before := r.NodesFor("some-key", 3)
	if len(before) != 3 {
		t.Fatalf("expected 3 nodes before removal, got %d", len(before))
	}

	r.RemoveNode(before[0])
	after := r.NodesFor("some-key", 2)
	if len(after) != 2 {
		t.Fatalf("expected 2 nodes after removal, got %d", len(after))
	}
	for _, n := range after {
		if n == before[0] {
			t.Fatalf("removed node %s should not appear in results", before[0])
		}
	}
}

func TestDistributionIsRoughlyBalanced(t *testing.T) {
	r := New()
	r.AddNode("a")
	r.AddNode("b")
	r.AddNode("c")

	counts := map[string]int{}
	const numKeys = 3000
	for i := 0; i < numKeys; i++ {
		primary := r.NodesFor(itoa(i)+"-key", 1)[0]
		counts[primary]++
	}

	// With 100 virtual nodes per physical node, distribution should be
	// close to even; allow generous slack since this is not a strict
	// correctness property, just a sanity check against a broken hash.
	expected := numKeys / 3
	for node, c := range counts {
		if c < expected/2 || c > expected*3/2 {
			t.Errorf("node %s got %d keys, expected roughly %d", node, c, expected)
		}
	}
}
