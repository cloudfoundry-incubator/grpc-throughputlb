package main

import (
	"os"

	throughputlb "code.cloudfoundry.org/grpc-throughputlb"
	"google.golang.org/grpc"
)

func main() {
	lb := throughputlb.NewThroughputLoadBalancer(20)

	conn, err := grpc.Dial(os.Getenv("GRPC_ADDR"),
		grpc.WithBalancer(lb))
	if err != nil {
		panic(err)
	}

	_ = conn
}
