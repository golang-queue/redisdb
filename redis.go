package redisdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/golang-queue/queue"
)

var _ queue.Worker = (*Worker)(nil)

// Option for queue system
type Option func(*Worker)

// Worker for Redis
type Worker struct {
	// redis config
	rdb              redis.Cmdable
	pubsub           *redis.PubSub
	channel          <-chan *redis.Message
	addr             string
	db               int
	connectionString string
	password         string
	channelName      string
	channelSize      int
	cluster          bool

	stopOnce sync.Once
	stop     chan struct{}
	runFunc  func(context.Context, queue.QueuedMessage) error
	logger   queue.Logger
	stopFlag int32
	metric   queue.Metric
}

func (w *Worker) incBusyWorker() {
	w.metric.IncBusyWorker()
}

func (w *Worker) decBusyWorker() {
	w.metric.DecBusyWorker()
}

// BusyWorkers return count of busy workers currently.
func (w *Worker) BusyWorkers() uint64 {
	return w.metric.BusyWorkers()
}

// WithAddr setup the addr of redis
func WithAddr(addr string) Option {
	return func(w *Worker) {
		w.addr = addr
	}
}

// WithPassword redis password
func WithDB(db int) Option {
	return func(w *Worker) {
		w.db = db
	}
}

// WithCluster redis cluster
func WithCluster(enable bool) Option {
	return func(w *Worker) {
		w.cluster = enable
	}
}

// WithChannelSize redis channel size
func WithChannelSize(size int) Option {
	return func(w *Worker) {
		w.channelSize = size
	}
}

// WithPassword redis password
func WithPassword(passwd string) Option {
	return func(w *Worker) {
		w.password = passwd
	}
}

// WithConnectionString redis connection string
func WithConnectionString(connectionString string) Option {
	return func(w *Worker) {
		w.connectionString = connectionString
	}
}

// WithChannel setup the channel of redis
func WithChannel(channel string) Option {
	return func(w *Worker) {
		w.channelName = channel
	}
}

// WithRunFunc setup the run func of queue
func WithRunFunc(fn func(context.Context, queue.QueuedMessage) error) Option {
	return func(w *Worker) {
		w.runFunc = fn
	}
}

// WithLogger set custom logger
func WithLogger(l queue.Logger) Option {
	return func(w *Worker) {
		w.logger = l
	}
}

// WithMetric set custom Metric
func WithMetric(m queue.Metric) Option {
	return func(w *Worker) {
		w.metric = m
	}
}

// NewWorker for struc
func NewWorker(opts ...Option) *Worker {
	var err error
	w := &Worker{
		addr:        "127.0.0.1:6379",
		channelName: "queue",
		channelSize: 1024,
		stop:        make(chan struct{}),
		logger:      queue.NewLogger(),
		runFunc: func(context.Context, queue.QueuedMessage) error {
			return nil
		},
		metric: queue.NewMetric(),
	}

	// Loop through each option
	for _, opt := range opts {
		// Call the option giving the instantiated
		opt(w)
	}

	if w.connectionString != "" {
		options, err := redis.ParseURL(w.connectionString)
		if err != nil {
			w.logger.Fatal(err)
		}
		w.rdb = redis.NewClient(options)
	} else if w.addr != "" {
		if w.cluster {
			w.rdb = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    strings.Split(w.addr, ","),
				Password: w.password,
			})
		} else {
			options := &redis.Options{
				Addr:     w.addr,
				Password: w.password,
				DB:       w.db,
			}
			w.rdb = redis.NewClient(options)
		}
	}

	_, err = w.rdb.Ping(context.Background()).Result()
	if err != nil {
		w.logger.Fatal(err)
	}

	ctx := context.Background()

	switch v := w.rdb.(type) {
	case *redis.Client:
		w.pubsub = v.Subscribe(ctx, w.channelName)
	case *redis.ClusterClient:
		w.pubsub = v.Subscribe(ctx, w.channelName)
	}

	var ropts []redis.ChannelOption

	if w.channelSize > 1 {
		ropts = append(ropts, redis.WithChannelSize(w.channelSize))
	}

	w.channel = w.pubsub.Channel(ropts...)
	// make sure the connection is successful
	if err := w.pubsub.Ping(ctx); err != nil {
		w.logger.Fatal(err)
	}

	return w
}

// BeforeRun run script before start worker
func (w *Worker) BeforeRun() error {
	return nil
}

// AfterRun run script after start worker
func (w *Worker) AfterRun() error {
	return nil
}

func (w *Worker) handle(job queue.Job) error {
	// create channel with buffer size 1 to avoid goroutine leak
	done := make(chan error, 1)
	panicChan := make(chan interface{}, 1)
	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), job.Timeout)
	w.incBusyWorker()
	defer func() {
		cancel()
		w.decBusyWorker()
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
		done <- w.runFunc(ctx, job)
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

// Capacity for channel
func (w *Worker) Capacity() int {
	return 0
}

// Usage for count of channel usage
func (w *Worker) Usage() int {
	return 0
}

// Queue send notification to queue
func (w *Worker) Queue(job queue.QueuedMessage) error {
	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return queue.ErrQueueShutdown
	}

	ctx := context.Background()

	// Publish a message.
	err := w.rdb.Publish(ctx, w.channelName, job.Bytes()).Err()
	if err != nil {
		return err
	}

	return nil
}

// Run start the worker
func (w *Worker) Run() error {
	for {
		// check queue status
		select {
		case <-w.stop:
			return nil
		default:
		}

		select {
		case m, ok := <-w.channel:
			select {
			case <-w.stop:
				return nil
			default:
			}

			if !ok {
				return fmt.Errorf("redis pubsub: channel=%s closed", w.channelName)
			}

			var data queue.Job
			if err := json.Unmarshal([]byte(m.Payload), &data); err != nil {
				w.logger.Error("json unmarshal error: ", err)
				continue
			}
			if err := w.handle(data); err != nil {
				w.logger.Error("handle job error: ", err)
			}
		case <-w.stop:
			return nil
		}
	}
}
