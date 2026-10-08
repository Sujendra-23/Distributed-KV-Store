package store

import "context"

// Backend is the storage-engine contract a node serves gRPC from. Two
// implementations exist: the in-memory Store (default, via MemoryBackend)
// and the DynamoDB-backed store in internal/dynamostore.
//
// Semantics every backend must honour:
//   - Put/Delete assign the next per-key version (previous version + 1).
//   - Delete writes a tombstone, never a hard delete.
//   - ApplyReplicated only applies a write whose version is strictly newer
//     than what is stored; it reports whether the write was applied.
type Backend interface {
	Put(ctx context.Context, key string, value []byte) (int64, error)
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Delete(ctx context.Context, key string) (int64, error)
	ApplyReplicated(ctx context.Context, key string, value []byte, version int64, tombstone bool) (bool, error)
	Count(ctx context.Context) (int, error)
}

// MemoryBackend adapts the in-memory Store to the Backend interface.
type MemoryBackend struct{ S *Store }

func NewMemoryBackend() *MemoryBackend { return &MemoryBackend{S: New()} }

func (m *MemoryBackend) Put(_ context.Context, key string, value []byte) (int64, error) {
	return m.S.Put(key, value), nil
}

func (m *MemoryBackend) Get(_ context.Context, key string) ([]byte, bool, error) {
	v, ok := m.S.Get(key)
	return v, ok, nil
}

func (m *MemoryBackend) Delete(_ context.Context, key string) (int64, error) {
	return m.S.Delete(key), nil
}

func (m *MemoryBackend) ApplyReplicated(_ context.Context, key string, value []byte, version int64, tombstone bool) (bool, error) {
	return m.S.ApplyReplicatedChecked(key, value, version, tombstone), nil
}

func (m *MemoryBackend) Count(_ context.Context) (int, error) { return m.S.Count(), nil }
