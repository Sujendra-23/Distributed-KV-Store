// Command node runs a single storage node in the cluster. It exposes the
// KVNode gRPC service and holds a shard of the overall keyspace in memory.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"time"

	"google.golang.org/grpc"

	"kvstore/internal/dynamostore"
	"kvstore/internal/store"
	pb "kvstore/proto"
)

type nodeServer struct {
	pb.UnimplementedKVNodeServer
	st        store.Backend
	startedAt time.Time
}

func (n *nodeServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if _, err := n.st.Put(ctx, req.Key, req.Value); err != nil {
		return &pb.PutResponse{Ok: false, Error: err.Error()}, err
	}
	return &pb.PutResponse{Ok: true}, nil
}

func (n *nodeServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	val, found, err := n.st.Get(ctx, req.Key)
	if err != nil {
		return &pb.GetResponse{Error: err.Error()}, err
	}
	return &pb.GetResponse{Found: found, Value: val}, nil
}

func (n *nodeServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if _, err := n.st.Delete(ctx, req.Key); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, err
	}
	return &pb.DeleteResponse{Ok: true}, nil
}

func (n *nodeServer) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	// A stale write is not an error: it is the last-writer-wins rule working.
	if _, err := n.st.ApplyReplicated(ctx, req.Key, req.Value, req.Version, req.Tombstone); err != nil {
		return &pb.ReplicateResponse{Ok: false}, err
	}
	return &pb.ReplicateResponse{Ok: true}, nil
}

func (n *nodeServer) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	count, err := n.st.Count(ctx)
	if err != nil {
		return nil, err // storage unreachable => node reports unhealthy to the coordinator
	}
	return &pb.HealthResponse{
		Alive:         true,
		KeyCount:      int32(count),
		UptimeSeconds: int64(time.Since(n.startedAt).Seconds()),
	}, nil
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

var nonAlnum = regexp.MustCompile(`[^A-Za-z0-9_.-]+`)

// newBackend selects the storage engine. "memory" (default) is the original
// in-process map; "dynamodb" persists to a DynamoDB table with conditional,
// version-checked writes.
func newBackend(kind, endpoint, region, table, addr string, createTable bool) (store.Backend, error) {
	switch kind {
	case "memory", "":
		return store.NewMemoryBackend(), nil
	case "dynamodb":
		if table == "" {
			table = "kvstore-" + nonAlnum.ReplaceAllString(addr, "_")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		db, err := dynamostore.NewClient(ctx, endpoint, region)
		if err != nil {
			return nil, fmt.Errorf("dynamodb client: %w", err)
		}
		ds := dynamostore.New(db, table)
		if createTable {
			if err := ds.EnsureTable(ctx); err != nil {
				return nil, err
			}
		}
		log.Printf("node: DynamoDB backend, table=%q endpoint=%q", table, endpoint)
		return ds, nil
	default:
		return nil, fmt.Errorf("unknown backend %q (want memory|dynamodb)", kind)
	}
}

func main() {
	addr := flag.String("addr", ":7001", "address to listen on")
	backend := flag.String("backend", envOr("KV_BACKEND", "memory"), "storage backend: memory (default) or dynamodb [env KV_BACKEND]")
	ddbEndpoint := flag.String("ddb-endpoint", os.Getenv("KV_DYNAMODB_ENDPOINT"), "DynamoDB endpoint override, e.g. http://localhost:8000 for DynamoDB Local/LocalStack; empty = real AWS [env KV_DYNAMODB_ENDPOINT]")
	ddbRegion := flag.String("ddb-region", envOr("AWS_REGION", ""), "AWS region [env AWS_REGION]")
	ddbTable := flag.String("ddb-table", os.Getenv("KV_DYNAMODB_TABLE"), "DynamoDB table; each node needs its own (default kvstore-<addr>) [env KV_DYNAMODB_TABLE]")
	ddbCreate := flag.Bool("ddb-create-table", true, "create the table at startup if it does not exist")
	flag.Parse()

	st, err := newBackend(*backend, *ddbEndpoint, *ddbRegion, *ddbTable, *addr, *ddbCreate)
	if err != nil {
		log.Fatalf("node: %v", err)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("node: failed to listen on %s: %v", *addr, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKVNodeServer(grpcServer, &nodeServer{
		st:        st,
		startedAt: time.Now(),
	})

	log.Printf("node: listening on %s", *addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("node: serve error: %v", err)
	}
}
