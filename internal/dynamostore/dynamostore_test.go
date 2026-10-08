package dynamostore

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These are integration tests against DynamoDB Local / LocalStack. They need
// KV_DYNAMODB_ENDPOINT (e.g. http://localhost:8000) and skip otherwise, so
// `go test ./...` stays green without Docker.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	ep := os.Getenv("KV_DYNAMODB_ENDPOINT")
	if ep == "" {
		t.Skip("KV_DYNAMODB_ENDPOINT not set; skipping DynamoDB integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := NewClient(ctx, ep, "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	s := New(db, fmt.Sprintf("kvtest-%d", time.Now().UnixNano()))
	if err := s.EnsureTable(ctx); err != nil {
		t.Fatalf("EnsureTable: %v", err)
	}
	return s
}

func TestPutGetVersionIncrements(t *testing.T) {
	s, ctx := newTestStore(t), context.Background()
	v1, err := s.Put(ctx, "k", []byte("a"))
	if err != nil || v1 != 1 {
		t.Fatalf("first put: v=%d err=%v", v1, err)
	}
	v2, _ := s.Put(ctx, "k", []byte("b"))
	if v2 != 2 {
		t.Fatalf("second put version = %d, want 2", v2)
	}
	got, ok, err := s.Get(ctx, "k")
	if err != nil || !ok || string(got) != "b" {
		t.Fatalf("get: %q ok=%v err=%v", got, ok, err)
	}
	if _, ok, _ := s.Get(ctx, "missing"); ok {
		t.Fatal("missing key reported found")
	}
}

// The core LWW guarantee: a replicated write with an older (or equal)
// version is rejected by DynamoDB's ConditionExpression and the newer value
// survives.
func TestStaleReplicatedWriteRejected(t *testing.T) {
	s, ctx := newTestStore(t), context.Background()
	if ok, err := s.ApplyReplicated(ctx, "k", []byte("new"), 5, false); err != nil || !ok {
		t.Fatalf("v5 apply: ok=%v err=%v", ok, err)
	}
	if ok, err := s.ApplyReplicated(ctx, "k", []byte("stale"), 3, false); err != nil || ok {
		t.Fatalf("stale v3 must be rejected: ok=%v err=%v", ok, err)
	}
	if ok, err := s.ApplyReplicated(ctx, "k", []byte("dup"), 5, false); err != nil || ok {
		t.Fatalf("equal-version v5 must be rejected: ok=%v err=%v", ok, err)
	}
	got, ok, _ := s.Get(ctx, "k")
	if !ok || string(got) != "new" {
		t.Fatalf("stale write clobbered value: %q ok=%v", got, ok)
	}
	if ok, _ := s.ApplyReplicated(ctx, "k", []byte("newer"), 6, false); !ok {
		t.Fatal("newer v6 should apply")
	}
	got, _, _ = s.Get(ctx, "k")
	if string(got) != "newer" {
		t.Fatalf("got %q want newer", got)
	}
}

func TestDeleteIsTombstoneAndBlocksResurrection(t *testing.T) {
	s, ctx := newTestStore(t), context.Background()
	s.Put(ctx, "k", []byte("a")) // v1
	vd, err := s.Delete(ctx, "k")
	if err != nil || vd != 2 {
		t.Fatalf("delete: v=%d err=%v", vd, err)
	}
	if _, ok, _ := s.Get(ctx, "k"); ok {
		t.Fatal("deleted key still readable")
	}
	// The row must still exist as a tombstone (not a hard delete)...
	it, err := s.read(ctx, "k")
	if err != nil || !it.exists || !it.tombstone || it.version != 2 {
		t.Fatalf("expected tombstone row v2, got %+v err=%v", it, err)
	}
	// ...which is what stops a late, older replicated write resurrecting it.
	if ok, _ := s.ApplyReplicated(ctx, "k", []byte("zombie"), 1, false); ok {
		t.Fatal("stale write resurrected a tombstoned key")
	}
	if _, ok, _ := s.Get(ctx, "k"); ok {
		t.Fatal("key resurrected")
	}
}

func TestCountExcludesTombstones(t *testing.T) {
	s, ctx := newTestStore(t), context.Background()
	s.Put(ctx, "a", []byte("1"))
	s.Put(ctx, "b", []byte("2"))
	s.Put(ctx, "c", []byte("3"))
	s.Delete(ctx, "a")
	n, err := s.Count(ctx)
	if err != nil || n != 2 {
		t.Fatalf("count=%d err=%v, want 2", n, err)
	}
}

// Concurrent local writers race on the same key; the conditional write must
// make versions unique and gap-free (1..N) with no lost updates.
func TestConcurrentPutsGetDistinctVersions(t *testing.T) {
	s, ctx := newTestStore(t), context.Background()
	const n = 8
	var wg sync.WaitGroup
	seen := make([]int64, n)
	var failed atomic.Int32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := s.Put(ctx, "hot", []byte(fmt.Sprint(i)))
			if err != nil {
				failed.Add(1)
				return
			}
			seen[i] = v
		}(i)
	}
	wg.Wait()
	if failed.Load() > 0 {
		t.Fatalf("%d puts failed", failed.Load())
	}
	uniq := map[int64]bool{}
	for _, v := range seen {
		if uniq[v] {
			t.Fatalf("duplicate version %d among %v", v, seen)
		}
		uniq[v] = true
	}
	it, _ := s.read(ctx, "hot")
	if it.version != n {
		t.Fatalf("final version %d, want %d (versions seen %v)", it.version, n, seen)
	}
}
