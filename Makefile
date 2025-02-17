PKG=github.com/cyverse/irodsfs-pool
VERSION=v0.11.0
GIT_COMMIT?=$(shell git rev-parse HEAD)
BUILD_DATE?=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS?="-X '${PKG}/commons.serviceVersion=${VERSION}' -X '${PKG}/commons.gitCommit=${GIT_COMMIT}' -X '${PKG}/commons.buildDate=${BUILD_DATE}'"
GO111MODULE=on
GOPROXY=direct
GOPATH=$(shell go env GOPATH)
OS_NAME:=$(shell grep -E '^ID=' /etc/os-release | cut -d'=' -f2 | tr -d '"')
SHELL:=/bin/bash
ADDUSER_FLAGS:=
ifeq (${OS_NAME},centos)
	ADDUSER_FLAGS=-r -d /dev/null -s /sbin/nologin 
else ifeq (${OS_NAME},almalinux)
	ADDUSER_FLAGS=-r -d /dev/null -s /sbin/nologin 
else ifeq (${OS_NAME},ubuntu)
	ADDUSER_FLAGS=--system --no-create-home --shell /sbin/nologin --group
else 
	ADDUSER_FLAGS=--system --no-create-home --shell /sbin/nologin --group
endif

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

.PHONY: install
install:
	cp bin/irodsfs-pool /usr/bin
	cp install/irodsfs-pool.service /usr/lib/systemd/system/
	id -u irodsfs-pool &> /dev/null || adduser ${ADDUSER_FLAGS} irodsfs-pool
	mkdir -p /etc/irodsfs-pool
	cp install/config.yaml /etc/irodsfs-pool
	chown irodsfs-pool:irodsfs-pool /etc/irodsfs-pool/config.yaml
	chmod 660 /etc/irodsfs-pool/config.yaml
	mkdir -p $$(awk '/data_root_path:/ {print $$2}' /etc/irodsfs-pool/config.yaml)
	chown irodsfs-pool:irodsfs-pool $$(awk '/data_root_path:/ {print $$2}' /etc/irodsfs-pool/config.yaml)

.PHONY: uninstall
uninstall:
	rm -f /usr/bin/irodsfs-pool
	rm -f /etc/systemd/system/multi-user.target.wants/irodsfs-pool.service || true
	rm -f /usr/lib/systemd/system/irodsfs-pool.service
	userdel irodsfs-pool || true
	groupdel irodsfs-pool || true
	rm -rf $$(awk '/data_root_path:/ {print $$2}' /etc/irodsfs-pool/config.yaml)
	rm -rf /etc/irodsfs-pool
