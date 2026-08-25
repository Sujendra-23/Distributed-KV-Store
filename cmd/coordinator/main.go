// Command coordinator is the cluster's entry point. It maps keys to storage
// nodes via consistent hashing, health-checks nodes, fans out replicated
// writes, and exposes a small HTTP API so clients don't need to speak gRPC
// or know about the ring themselves.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "kvstore/proto"
	"kvstore/internal/hashring"
)

const (
	replicationFactor = 2
	healthInterval    = 3 * time.Second
	rpcTimeout        = 2 * time.Second
)

type coordinator struct {
	ring     *hashring.Ring
	mu       sync.RWMutex
	conns    map[string]pb.KVNodeClient
	rawConns map[string]*grpc.ClientConn
}

func newCoordinator(nodeAddrs []string) *coordinator {
	c := &coordinator{
		ring:     hashring.New(),
		conns:    make(map[string]pb.KVNodeClient),
		rawConns: make(map[string]*grpc.ClientConn),
	}
	for _, addr := range nodeAddrs {
		c.connect(addr)
	}
	return c
}

func (c *coordinator) connect(addr string) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("coordinator: could not dial %s: %v", addr, err)
		return
	}
	c.mu.Lock()
	c.conns[addr] = pb.NewKVNodeClient(conn)
	c.rawConns[addr] = conn
	c.mu.Unlock()
}

// healthLoop polls every known node and keeps the hash ring in sync with
// which nodes are actually alive, so a dead node stops receiving new keys
// and the cluster keeps serving traffic through the remaining replicas.
func (c *coordinator) healthLoop() {
	c.checkAllNodes() // run once immediately so the ring is populated before we start serving
	ticker := time.NewTicker(healthInterval)
	for range ticker.C {
		c.checkAllNodes()
	}
}

func (c *coordinator) checkAllNodes() {
	c.mu.RLock()
	targets := make(map[string]pb.KVNodeClient, len(c.conns))
	for addr, cl := range c.conns {
		targets[addr] = cl
	}
	c.mu.RUnlock()

	for addr, cl := range targets {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		_, err := cl.Health(ctx, &pb.HealthRequest{})
		cancel()
		if err != nil {
			log.Printf("coordinator: node %s failed health check, removing from ring: %v", addr, err)
			c.ring.RemoveNode(addr)
		} else {
			c.ring.AddNode(addr)
		}
	}
}

func (c *coordinator) clientFor(addr string) pb.KVNodeClient {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conns[addr]
}

// put writes to the primary node for the key, then replicates to the
// remaining nodes in the key's replica set.
func (c *coordinator) put(key string, value []byte) error {
	targets := c.ring.NodesFor(key, replicationFactor)
	if len(targets) == 0 {
		return errNoNodes
	}
	primary := targets[0]
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := c.clientFor(primary).Put(ctx, &pb.PutRequest{Key: key, Value: value})
	if err != nil || !resp.Ok {
		return err
	}
	for _, replica := range targets[1:] {
		rctx, rcancel := context.WithTimeout(context.Background(), rpcTimeout)
		_, _ = c.clientFor(replica).Replicate(rctx, &pb.ReplicateRequest{
			Key: key, Value: value, Version: time.Now().UnixNano(),
		})
		rcancel()
	}
	return nil
}

func (c *coordinator) get(key string) ([]byte, bool, error) {
	targets := c.ring.NodesFor(key, replicationFactor)
	var lastErr error
	for _, addr := range targets {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		resp, err := c.clientFor(addr).Get(ctx, &pb.GetRequest{Key: key})
		cancel()
		if err != nil {
			lastErr = err
			continue // primary might be down, try the next replica
		}
		if resp.Found {
			return resp.Value, true, nil
		}
	}
	return nil, false, lastErr
}

func (c *coordinator) delete(key string) error {
	targets := c.ring.NodesFor(key, replicationFactor)
	if len(targets) == 0 {
		return errNoNodes
	}
	for _, addr := range targets {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		_, err := c.clientFor(addr).Delete(ctx, &pb.DeleteRequest{Key: key})
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

var errNoNodes = httpError("no storage nodes available")

type httpError string

func (e httpError) Error() string { return string(e) }

// --- HTTP layer ---

func (c *coordinator) handlePut(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	body := make([]byte, r.ContentLength)
	if _, err := r.Body.Read(body); err != nil && r.ContentLength > 0 {
		// short reads on the last chunk are normal; only bail on real errors
	}
	if err := c.put(key, body); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (c *coordinator) handleGet(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	val, found, err := c.get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if !found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Write(val)
}

func (c *coordinator) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if err := c.delete(key); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (c *coordinator) handleKV(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		c.handlePut(w, r)
	case http.MethodGet:
		c.handleGet(w, r)
	case http.MethodDelete:
		c.handleDelete(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (c *coordinator) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"members": c.ring.Members(),
	})
}

func main() {
	httpAddr := flag.String("http", ":8080", "HTTP address for client API")
	nodes := flag.String("nodes", "localhost:7001,localhost:7002,localhost:7003", "comma-separated storage node addresses")
	flag.Parse()

	c := newCoordinator(strings.Split(*nodes, ","))
	go c.healthLoop()
	// Give the first health pass a moment to run before serving traffic.
	time.Sleep(500 * time.Millisecond)

	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", c.handleKV)
	mux.HandleFunc("/status", c.handleStatus)

	log.Printf("coordinator: HTTP API on %s, nodes=%v", *httpAddr, strings.Split(*nodes, ","))
	log.Fatal(http.ListenAndServe(*httpAddr, mux))
}
