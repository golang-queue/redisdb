package redisdb

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
	"github.com/golang-queue/queue/job"

	"github.com/redis/go-redis/v9"
	"github.com/yassinebenaid/godump"
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

// NewWorker creates a new Worker instance with the provided options.
// It initializes a Redis client based on the options and establishes a connection to the Redis server.
// The Worker is responsible for subscribing to a Redis channel and receiving messages from it.
// It returns the created Worker instance.
func NewWorker(opts ...Option) *Worker {
	var err error
	w := &Worker{
		opts: newOptions(opts...),
		stop: make(chan struct{}),
	}

	if w.opts.debug {
		_ = godump.Dump(w.opts)
	}

	options := &redis.Options{
		Addr:      w.opts.addr,
		Username:  w.opts.username,
		Password:  w.opts.password,
		DB:        w.opts.db,
		TLSConfig: w.opts.tls,
	}
	w.rdb = redis.NewClient(options)

	if w.opts.connectionString != "" {
		options, err := redis.ParseURL(w.opts.connectionString)
		if err != nil {
			w.opts.logger.Fatal(err)
		}
		w.rdb = redis.NewClient(options)
	}

	if w.opts.cluster {
		w.rdb = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:     strings.Split(w.opts.addr, ","),
			Username:  w.opts.username,
			Password:  w.opts.password,
			TLSConfig: w.opts.tls,
		})
	}

	if w.opts.sentinel {
		w.rdb = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    w.opts.masterName,
			SentinelAddrs: strings.Split(w.opts.addr, ","),
			Username:      w.opts.username,
			Password:      w.opts.password,
			DB:            w.opts.db,
			TLSConfig:     w.opts.tls,
		})
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

// Run to execute new task
func (w *Worker) Run(ctx context.Context, task core.QueuedMessage) error {
	return w.opts.runFunc(ctx, task)
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
			var data job.Message
			_ = json.Unmarshal([]byte(task.Payload), &data)
			return &data, nil
		case <-time.After(1 * time.Second):
			if clock == 5 {
				break loop
			}
			clock += 1
		}
	}

	return nil, queue.ErrNoTaskInQueue
}
