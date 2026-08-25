// Command node runs a single storage node in the cluster. It exposes the
// KVNode gRPC service and holds a shard of the overall keyspace in memory.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"

	pb "kvstore/proto"
	"kvstore/internal/store"
)

type nodeServer struct {
	pb.UnimplementedKVNodeServer
	st        *store.Store
	startedAt time.Time
}

func (n *nodeServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	n.st.Put(req.Key, req.Value)
	return &pb.PutResponse{Ok: true}, nil
}

func (n *nodeServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	val, found := n.st.Get(req.Key)
	return &pb.GetResponse{Found: found, Value: val}, nil
}

func (n *nodeServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	n.st.Delete(req.Key)
	return &pb.DeleteResponse{Ok: true}, nil
}

func (n *nodeServer) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	n.st.ApplyReplicated(req.Key, req.Value, req.Version, req.Tombstone)
	return &pb.ReplicateResponse{Ok: true}, nil
}

func (n *nodeServer) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{
		Alive:         true,
		KeyCount:      int32(n.st.Count()),
		UptimeSeconds: int64(time.Since(n.startedAt).Seconds()),
	}, nil
}

func main() {
	addr := flag.String("addr", ":7001", "address to listen on")
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("node: failed to listen on %s: %v", *addr, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKVNodeServer(grpcServer, &nodeServer{
		st:        store.New(),
		startedAt: time.Now(),
	})

	log.Printf("node: listening on %s", *addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("node: serve error: %v", err)
	}
}
