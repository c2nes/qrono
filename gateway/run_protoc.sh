#!/bin/sh

set -eu

protoc -I third_party/github.com/googleapis/api-common-protos -I. \
       --go_out=plugins=grpc,paths=source_relative:. \
       proto/api.proto

protoc -I third_party/github.com/googleapis/api-common-protos -I. \
       --grpc-gateway_out=logtostderr=true,paths=source_relative:. \
       proto/api.proto
