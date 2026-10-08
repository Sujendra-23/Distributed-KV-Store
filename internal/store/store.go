// Package store implements the in-memory storage engine that backs each node.
package store

import "sync"

type entry struct {
	value     []byte
	version   int64
	tombstone bool
}

// Store is a thread-safe key/value map with last-writer-wins conflict
// resolution based on a per-key version number.
type Store struct {
	mu   sync.RWMutex
	data map[string]entry
}

func New() *Store {
	return &Store{data: make(map[string]entry)}
}

// Put writes a key at a new version and returns that version number.
// The caller (the gRPC handler) is responsible for propagating the same
// version to replicas via Replicate so all copies converge.
func (s *Store) Put(key string, value []byte) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.data[key].version + 1
	s.data[key] = entry{value: value, version: v}
	return v
}

// ApplyReplicated writes a value that arrived from another node's
// Replicate RPC, but only if it's newer than what's already stored -
// this is what makes last-writer-wins safe under concurrent writes.
func (s *Store) ApplyReplicated(key string, value []byte, version int64, tombstone bool) {
	s.ApplyReplicatedChecked(key, value, version, tombstone)
}

// ApplyReplicatedChecked is ApplyReplicated that also reports whether the
// write was applied (false means it was stale and ignored).
func (s *Store) ApplyReplicatedChecked(key string, value []byte, version int64, tombstone bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.data[key]; ok && existing.version >= version {
		return false // stale write, ignore
	}
	s.data[key] = entry{value: value, version: version, tombstone: tombstone}
	return true
}

func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok || e.tombstone {
		return nil, false
	}
	return e.value, true
}

// Delete marks a key as removed and returns the new version, a tombstone
// rather than a hard delete so replicas don't resurrect the key.
func (s *Store) Delete(key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.data[key].version + 1
	s.data[key] = entry{version: v, tombstone: true}
	return v
}

func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, e := range s.data {
		if !e.tombstone {
			n++
		}
	}
	return n
}
