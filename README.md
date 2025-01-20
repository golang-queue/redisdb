# redis

[![Run Testing](https://github.com/golang-queue/redisdb/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/golang-queue/redisdb/actions/workflows/go.yml)
[![codecov](https://codecov.io/gh/golang-queue/redisdb/branch/main/graph/badge.svg?token=FFZN8E2ZZB)](https://codecov.io/gh/golang-queue/redisdb)
[![Go Report Card](https://goreportcard.com/badge/github.com/golang-queue/redisdb)](https://goreportcard.com/report/github.com/golang-queue/redisdb)

Redis [Pub/Sub](https://redis.io/docs/manual/pubsub/) as backend for [Queue package](https://github.com/golang-queue/queue)

## Setup

start the redis server

```sh
redis-server
```

![screen](/images/screen.png)

start the redis cluster, see the [config](./conf/redis.conf)

```sh
# server 01
mkdir server01 && cd server01 && redis-server redis.conf --port 6379
# server 02
mkdir server02 && cd server02 && redis-server redis.conf --port 6380
```

## Example

For single server

```go
package main

import (
  "context"
  "encoding/json"
  "fmt"
  "log"
  "time"

  "github.com/golang-queue/queue"
  "github.com/golang-queue/redisdb"
)

type job struct {
  Message string
}

func (j *job) Bytes() []byte {
  b, err := json.Marshal(j)
  if err != nil {
    panic(err)
  }
  return b
}

func main() {
  taskN := 100
  rets := make(chan string, taskN)

  // define the worker
  w := redisdb.NewWorker(
    redisdb.WithAddr("127.0.0.1:6379"),
    redisdb.WithChannel("foobar"),
    redisdb.WithRunFunc(func(ctx context.Context, m queue.TaskMessage) error {
      var v job
      if err := json.Unmarshal(m.Payload(), &v); err != nil {
        return err
      }

      rets <- v.Message
      return nil
    }),
  )

  // define the queue
  q := queue.NewPool(
    5,
    queue.WithWorker(w),
  )

  // assign tasks in queue
  for i := 0; i < taskN; i++ {
    go func(i int) {
      if err := q.Queue(&job{
        Message: fmt.Sprintf("handle the job: %d", i+1),
      }); err != nil {
        log.Fatal(err)
      }
    }(i)
  }

  // wait until all tasks done
  for i := 0; i < taskN; i++ {
    fmt.Println("message:", <-rets)
    time.Sleep(50 * time.Millisecond)
  }

  // shutdown the service and notify all the worker
  q.Release()
}
```

## Testing

```sh
go test -v ./...
```
