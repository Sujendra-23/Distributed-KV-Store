// Command healthcheck is a small debug utility (not part of the cluster
// itself) for querying a node's Health RPC directly, useful when checking
// per-node key counts during manual testing.
package main

import (
	"context"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "kvstore/proto"
)

func main() {
	for _, addr := range os.Args[1:] {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Println(addr, "dial err", err)
			continue
		}
		cl := pb.NewKVNodeClient(conn)
		resp, err := cl.Health(context.Background(), &pb.HealthRequest{})
		if err != nil {
			fmt.Println(addr, "health err", err)
			continue
		}
		fmt.Printf("%s -> alive=%v key_count=%d uptime=%ds\n", addr, resp.Alive, resp.KeyCount, resp.UptimeSeconds)
	}
}
