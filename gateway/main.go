package main

//go:generate ./scripts/generate_protos.sh

import (
	"flag"
	"net/http"

	gw "git.thunes.dev/queue-server/gateway/proto"
	"github.com/golang/glog"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	target = flag.String("target", "localhost:16381", "gRPC backend address")
	listen = flag.String("listen", "localhost:16380", "listen address")
)

func run() error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithInsecure()}
	err := gw.RegisterQueueServerHandlerFromEndpoint(ctx, mux, *target, opts)
	if err != nil {
		return err
	}

	return http.ListenAndServe(*listen, mux)
}

func main() {
	flag.Parse()
	defer glog.Flush()

	if err := run(); err != nil {
		glog.Fatal(err)
	}
}
