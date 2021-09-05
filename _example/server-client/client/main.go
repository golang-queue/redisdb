package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	taskN := 10000
	rets := make(chan string, taskN)

	// define the worker
	w := redisdb.NewWorker(
		redisdb.WithAddr("127.0.0.1:6379"),
		redisdb.WithChannel("foobar"),
		redisdb.WithRunFunc(func(ctx context.Context, m queue.QueuedMessage) error {
			v, ok := m.(*job)
			if !ok {
				if err := json.Unmarshal(m.Bytes(), &v); err != nil {
					return err
				}
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

	// wait until all tasks done
	for i := 0; i < taskN; i++ {
		fmt.Println("message:", <-rets)
		time.Sleep(50 * time.Millisecond)
	}

	// shutdown the service and notify all the worker
	q.Release()
}
