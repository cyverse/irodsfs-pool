PKG=github.com/cyverse/irodsfs-pool
VERSION=v0.3.3
GIT_COMMIT?=$(shell git rev-parse HEAD)
BUILD_DATE?=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS?="-X '${PKG}/commons.serviceVersion=${VERSION}' -X '${PKG}/commons.gitCommit=${GIT_COMMIT}' -X '${PKG}/commons.buildDate=${BUILD_DATE}'"
GO111MODULE=on
GOPROXY=direct
GOPATH=$(shell go env GOPATH)

.EXPORT_ALL_VARIABLES:

.PHONY: build
build:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux go build -ldflags=${LDFLAGS} -o bin/irodsfs-pool ./cmd/

.PHONY: protobuf
protobuf:
	export PATH=$$PATH:${GOPATH}/bin; protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative service/api/api.proto


.PHONY: examples
examples:
	CGO_ENABLED=0 GOOS=linux go build -ldflags=${LDFLAGS} -o ./client_examples/list_dir/list_dir.out ./client_examples/list_dir/list_dir.go
	