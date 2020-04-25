#!/bin/sh

set -eu

PROTO_FLAGS="-I third_party/github.com/googleapis/api-common-protos -I."

protoc $PROTO_FLAGS \
       --go_out=plugins=grpc,paths=source_relative:. \
       proto/api.proto

protoc $PROTO_FLAGS \
       --grpc-gateway_out=logtostderr=true,paths=source_relative:. \
       proto/api.proto
