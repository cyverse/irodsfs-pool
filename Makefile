PKG=github.com/cyverse/irodsfs-pool
VERSION=v0.8.5
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
# This requires installation of two modules to compile protobuf and grpc
# sudo apt install protobuf-compiler
# go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
# go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
	export PATH=${PATH}:${GOPATH}/bin; protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative service/api/api.proto


.PHONY: examples
examples:
	CGO_ENABLED=0 GOOS=linux go build -ldflags=${LDFLAGS} -o ./client_examples/list_dir/list_dir.out ./client_examples/list_dir/list_dir.go


.PHONY: release
release: build
	mkdir -p release
	mkdir -p release/bin
	cp bin/irodsfs-pool release/bin
	mkdir -p release/install
	cp install/config.yaml release/install
	cp install/irodsfs-pool.service release/install
	cp install/README.md release/install
	cp Makefile.release release/Makefile
	cd release && tar zcvf ../irodsfs-pool.tar.gz *

.PHONY: install_centos
install_centos:
	cp bin/irodsfs-pool /usr/bin
	cp install/irodsfs-pool.service /usr/lib/systemd/system/
	id -u irodsfs-pool &> /dev/null || adduser -r -d /dev/null -s /sbin/nologin irodsfs-pool
	mkdir -p /etc/irodsfs-pool
	cp install/config.yaml /etc/irodsfs-pool
	chown irodsfs-pool /etc/irodsfs-pool/config.yaml
	chmod 660 /etc/irodsfs-pool/config.yaml

.PHONY: install_ubuntu
install_ubuntu:
	cp bin/irodsfs-pool /usr/bin
	cp install/irodsfs-pool.service /etc/systemd/system/
	id -u irodsfs-pool &> /dev/null || adduser --system --home /dev/null --shell /sbin/nologin irodsfs-pool
	mkdir -p /etc/irodsfs-pool
	cp install/config.yaml /etc/irodsfs-pool
	chown irodsfs-pool /etc/irodsfs-pool/config.yaml
	chmod 660 /etc/irodsfs-pool/config.yaml

.PHONY: uninstall
uninstall:
	rm -f /usr/bin/irodsfs-pool
	rm -f /etc/systemd/system/irodsfs-pool.service
	rm -f /usr/lib/systemd/system/irodsfs-pool.service
	userdel irodsfs-pool | true
	rm -rf /etc/irodsfs-pool
