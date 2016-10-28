#!/bin/bash
CWD=$(pwd)
GOOS=linux
GOARCH=amd64
CGO_ENABLED=0
export GOOS GOARCH CGO_ENABLED
go build -o mqueue-$VERSION -ldflags "-X main.version=$VERSION`date -u +.%Y%m%d.%H%M%S`.`git rev-parse --short HEAD`" github.com/secmask/mqueue/cmd/mqueue
