#!/usr/bin/make -f

export CGO_ENABLED=0
export GO111MODULE=on

PROJECT=github.com/skpr/cloudfront-cloudwatchlogs
VERSION=$(shell git describe --tags --always)
COMMIT=$(shell git rev-list -1 HEAD)

default: lint test build

# Run all lint checking with exit codes for CI.
lint:
	golint -set_exit_status `go list ./... | grep -v /vendor/`

# Run go fmt against code
fmt:
	go fmt ./...

# Run tests with coverage reporting.
test:
	go test -cover ./...

build:
	gox -os='linux darwin' \
	    -arch='amd64' \
	    -output='bin/cloudfront-cloudwatchlogs_{{.OS}}_{{.Arch}}' \
	    -ldflags='-extldflags "-static" -X github.com/skpr/cloudfront-cloudwatchlogs/cmd/version.GitVersion=$(VERSION) -X github.com/skpr/cloudfront-cloudwatchlogs/cmd/version.GitCommit=$(COMMIT)' \
	    $(PROJECT)