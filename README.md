# redis

[![Run CI Lint](https://github.com/golang-queue/redisdb/actions/workflows/lint.yml/badge.svg)](https://github.com/golang-queue/redisdb/actions/workflows/lint.yml)
[![Run Testing](https://github.com/golang-queue/redisdb/actions/workflows/testing.yml/badge.svg)](https://github.com/golang-queue/redisdb/actions/workflows/testing.yml)

Redis as backend for [Queue package](https://github.com/golang-queue/queue)

## Setup

start the redis server

```sh
redis-server
```

![screen](/images/screen.png)

## Testing

```sh
go test -v ./...
```
