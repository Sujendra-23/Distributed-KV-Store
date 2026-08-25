// Package hashring implements consistent hashing with virtual nodes so that
// adding or removing a storage node only reshuffles a small fraction of keys
// instead of the whole keyspace.
package hashring

import (
	"crypto/sha1"
	"encoding/binary"
	"sort"
	"sync"
)

const virtualNodesPerNode = 100

// Ring maps hash positions to physical node addresses.
type Ring struct {
	mu       sync.RWMutex
	sorted   []uint32          // sorted virtual node hash positions
	posToNode map[uint32]string // virtual position -> physical node address
	nodes    map[string]bool   // set of physical nodes currently in the ring
}

func New() *Ring {
	return &Ring{
		posToNode: make(map[uint32]string),
		nodes:     make(map[string]bool),
	}
}

func hashKey(s string) uint32 {
	h := sha1.Sum([]byte(s))
	return binary.BigEndian.Uint32(h[:4])
}

// AddNode inserts a physical node (e.g. "node-1:7001") into the ring.
func (r *Ring) AddNode(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nodes[addr] {
		return
	}
	r.nodes[addr] = true
	for i := 0; i < virtualNodesPerNode; i++ {
		vpos := hashKey(addr + "#" + itoa(i))
		r.posToNode[vpos] = addr
		r.sorted = append(r.sorted, vpos)
	}
	sort.Slice(r.sorted, func(i, j int) bool { return r.sorted[i] < r.sorted[j] })
}

// RemoveNode takes a physical node out of the ring, e.g. after it fails
// health checks. Keys that hashed to it fall through to the next node.
func (r *Ring) RemoveNode(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.nodes[addr] {
		return
	}
	delete(r.nodes, addr)
	newSorted := r.sorted[:0]
	for _, pos := range r.sorted {
		if r.posToNode[pos] == addr {
			delete(r.posToNode, pos)
			continue
		}
		newSorted = append(newSorted, pos)
	}
	r.sorted = newSorted
}

// NodesFor returns the primary node for a key plus (replicas-1) distinct
// follower nodes walked clockwise around the ring, e.g. NodesFor(k, 2)
// returns [primary, firstReplica].
func (r *Ring) NodesFor(key string, replicas int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sorted) == 0 {
		return nil
	}
	h := hashKey(key)
	idx := sort.Search(len(r.sorted), func(i int) bool { return r.sorted[i] >= h })
	if idx == len(r.sorted) {
		idx = 0
	}

	seen := make(map[string]bool)
	var result []string
	for i := 0; i < len(r.sorted) && len(result) < replicas; i++ {
		pos := r.sorted[(idx+i)%len(r.sorted)]
		node := r.posToNode[pos]
		if !seen[node] {
			seen[node] = true
			result = append(result, node)
		}
	}
	return result
}

// Members returns the current set of physical nodes.
func (r *Ring) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func itoa(i int) string {
	// tiny local itoa to avoid importing strconv just for this
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [12]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
