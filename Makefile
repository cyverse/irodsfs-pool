PKG=github.com/cyverse/irodsfs-proxy
GO111MODULE=on
GOPROXY=direct
GOPATH=$(shell go env GOPATH)

.EXPORT_ALL_VARIABLES:

.PHONY: build
build:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux go build -o bin/irodsfs-proxy ./cmd/

.PHONY: protobuf
protobuf:
	export PATH=$$PATH:${GOPATH}/bin; protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative service/api/api.proto


.PHONY: examples
examples:
	CGO_ENABLED=0 GOOS=linux go build -o ./client_examples/list_dir/list_dir.out ./client_examples/list_dir/list_dir.go
	