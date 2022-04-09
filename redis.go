package redisdb

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
)

var _ core.Worker = (*Worker)(nil)

// Worker for Redis
type Worker struct {
	// redis config
	rdb      redis.Cmdable
	pubsub   *redis.PubSub
	channel  <-chan *redis.Message
	stopFlag int32
	stopOnce sync.Once
	stop     chan struct{}
	opts     options
}

// NewWorker for struc
func NewWorker(opts ...Option) *Worker {
	var err error
	w := &Worker{
		opts: newOptions(opts...),
		stop: make(chan struct{}),
	}

	if w.opts.connectionString != "" {
		options, err := redis.ParseURL(w.opts.connectionString)
		if err != nil {
			w.opts.logger.Fatal(err)
		}
		w.rdb = redis.NewClient(options)
	} else if w.opts.addr != "" {
		if w.opts.cluster {
			w.rdb = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    strings.Split(w.opts.addr, ","),
				Password: w.opts.password,
			})
		} else {
			options := &redis.Options{
				Addr:     w.opts.addr,
				Password: w.opts.password,
				DB:       w.opts.db,
			}
			w.rdb = redis.NewClient(options)
		}
	}

	_, err = w.rdb.Ping(context.Background()).Result()
	if err != nil {
		w.opts.logger.Fatal(err)
	}

	ctx := context.Background()

	switch v := w.rdb.(type) {
	case *redis.Client:
		w.pubsub = v.Subscribe(ctx, w.opts.channelName)
	case *redis.ClusterClient:
		w.pubsub = v.Subscribe(ctx, w.opts.channelName)
	}

	var ropts []redis.ChannelOption

	if w.opts.channelSize > 1 {
		ropts = append(ropts, redis.WithChannelSize(w.opts.channelSize))
	}

	w.channel = w.pubsub.Channel(ropts...)
	// make sure the connection is successful
	if err := w.pubsub.Ping(ctx); err != nil {
		w.opts.logger.Fatal(err)
	}

	return w
}

func (w *Worker) handle(job queue.Job) error {
	// create channel with buffer size 1 to avoid goroutine leak
	done := make(chan error, 1)
	panicChan := make(chan interface{}, 1)
	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), job.Timeout)
	defer func() {
		cancel()
	}()

	// run the job
	go func() {
		// handle panic issue
		defer func() {
			if p := recover(); p != nil {
				panicChan <- p
			}
		}()

		// run custom process function
		done <- w.opts.runFunc(ctx, job)
	}()

	select {
	case p := <-panicChan:
		panic(p)
	case <-ctx.Done(): // timeout reached
		return ctx.Err()
	case <-w.stop: // shutdown service
		// cancel job
		cancel()

		leftTime := job.Timeout - time.Since(startTime)
		// wait job
		select {
		case <-time.After(leftTime):
			return context.DeadlineExceeded
		case err := <-done: // job finish
			return err
		case p := <-panicChan:
			panic(p)
		}
	case err := <-done: // job finish
		return err
	}
}

// Shutdown worker
func (w *Worker) Shutdown() error {
	if !atomic.CompareAndSwapInt32(&w.stopFlag, 0, 1) {
		return queue.ErrQueueShutdown
	}

	w.stopOnce.Do(func() {
		w.pubsub.Close()
		switch v := w.rdb.(type) {
		case *redis.Client:
			v.Close()
		case *redis.ClusterClient:
			v.Close()
		}
		close(w.stop)
	})
	return nil
}

// Queue send notification to queue
func (w *Worker) Queue(job core.QueuedMessage) error {
	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return queue.ErrQueueShutdown
	}

	ctx := context.Background()

	// Publish a message.
	err := w.rdb.Publish(ctx, w.opts.channelName, job.Bytes()).Err()
	if err != nil {
		return err
	}

	return nil
}

// Run start the worker
func (w *Worker) Run(task core.QueuedMessage) error {
	data, _ := task.(queue.Job)

	if err := w.handle(data); err != nil {
		return err
	}

	return nil
}

// Request a new task
func (w *Worker) Request() (core.QueuedMessage, error) {
	clock := 0
loop:
	for {
		select {
		case task, ok := <-w.channel:
			if !ok {
				return nil, queue.ErrQueueHasBeenClosed
			}
			var data queue.Job
			_ = json.Unmarshal([]byte(task.Payload), &data)
			return data, nil
		case <-time.After(1 * time.Second):
			if clock == 5 {
				break loop
			}
			clock += 1
		}
	}

	return nil, queue.ErrNoTaskInQueue
}
