package store

import "testing"

func TestPutGet(t *testing.T) {
	s := New()
	s.Put("k", []byte("v1"))
	val, ok := s.Get("k")
	if !ok || string(val) != "v1" {
		t.Fatalf("expected v1, got %q ok=%v", val, ok)
	}
}

func TestDeleteMakesGetReturnNotFound(t *testing.T) {
	s := New()
	s.Put("k", []byte("v1"))
	s.Delete("k")
	_, ok := s.Get("k")
	if ok {
		t.Fatalf("expected not found after delete")
	}
}

func TestApplyReplicatedRejectsStaleVersion(t *testing.T) {
	s := New()
	s.ApplyReplicated("k", []byte("new"), 5, false)
	// A replicated write with a lower version than what's stored must be
	// ignored - this is what makes last-writer-wins safe under concurrent
	// writes landing on replicas out of order.
	s.ApplyReplicated("k", []byte("stale"), 3, false)

	val, ok := s.Get("k")
	if !ok || string(val) != "new" {
		t.Fatalf("expected stale write to be rejected, got %q ok=%v", val, ok)
	}
}

func TestApplyReplicatedAppliesNewerVersion(t *testing.T) {
	s := New()
	s.ApplyReplicated("k", []byte("old"), 1, false)
	s.ApplyReplicated("k", []byte("newer"), 2, false)

	val, ok := s.Get("k")
	if !ok || string(val) != "newer" {
		t.Fatalf("expected newer write to apply, got %q ok=%v", val, ok)
	}
}

func TestCountExcludesTombstones(t *testing.T) {
	s := New()
	s.Put("a", []byte("1"))
	s.Put("b", []byte("2"))
	s.Delete("a")

	if got := s.Count(); got != 1 {
		t.Fatalf("expected count=1 after deleting one of two keys, got %d", got)
	}
}
