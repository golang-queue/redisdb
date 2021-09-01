package redisdb

import (
	"context"
	"testing"
	"time"

	"github.com/golang-queue/queue"

	"github.com/stretchr/testify/assert"
)

var host = "127.0.0.1"

type mockMessage struct {
	Message string
}

func (m mockMessage) Bytes() []byte {
	return []byte(m.Message)
}

func TestRedisDefaultFlow(t *testing.T) {
	m := &mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(host+":6379"),
		WithChannel("test"),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(100 * time.Millisecond)
	assert.NoError(t, q.Queue(m))
	m.Message = "bar"
	assert.NoError(t, q.Queue(m))
	q.Shutdown()
	q.Wait()
}

func TestNSQShutdown(t *testing.T) {
	w := NewWorker(
		WithAddr(host+":6379"),
		WithChannel("test2"),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(1 * time.Second)
	q.Shutdown()
	// check shutdown once
	assert.Error(t, w.Shutdown())
	assert.Equal(t, queue.ErrQueueShutdown, w.Shutdown())
	q.Wait()
}

func TestGoroutinePanic(t *testing.T) {
	m := mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(host+":6379"),
		WithChannel("GoroutinePanic"),
		WithRunFunc(func(ctx context.Context, m queue.QueuedMessage) error {
			panic("missing something")
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(50 * time.Millisecond)
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	time.Sleep(200 * time.Millisecond)
	q.Shutdown()
	assert.Error(t, q.Queue(m))
	q.Wait()
}
