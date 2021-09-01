package redisdb

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-queue/queue"

	"github.com/go-redis/redis/v8"
)

var _ queue.Worker = (*Worker)(nil)

// Option for queue system
type Option func(*Worker)

// Worker for Redis
type Worker struct {
	// redis config
	rdb              *redis.Client
	pubsub           *redis.PubSub
	addr             string
	db               int
	connectionString string
	password         string
	channel          string
	channelSize      int

	stopOnce  sync.Once
	stop      chan struct{}
	runFunc   func(context.Context, queue.QueuedMessage) error
	logger    queue.Logger
	stopFlag  int32
	startFlag int32
}

// WithAddr setup the addr of NSQ
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

// WithChannelSize redis password
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

// WithPassword redis password
func WithConnectionString(connectionString string) Option {
	return func(w *Worker) {
		w.connectionString = connectionString
	}
}

// WithChannel setup the channel of NSQ
func WithChannel(channel string) Option {
	return func(w *Worker) {
		w.channel = channel
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

// NewWorker for struc
func NewWorker(opts ...Option) *Worker {
	var err error
	w := &Worker{
		addr:        "127.0.0.1:6379",
		channel:     "queue",
		channelSize: 1024,
		stop:        make(chan struct{}),
		logger:      queue.NewLogger(),
		runFunc: func(context.Context, queue.QueuedMessage) error {
			return nil
		},
	}

	// Loop through each option
	for _, opt := range opts {
		// Call the option giving the instantiated
		opt(w)
	}

	var options *redis.Options

	if w.connectionString != "" {
		options, err = redis.ParseURL(w.connectionString)
		if err != nil {
			w.logger.Fatal(err)
		}
	} else if w.addr != "" {
		options = &redis.Options{
			Addr:     w.addr,
			Password: w.password,
			DB:       w.db,
		}
	}

	rdb := redis.NewClient(options)

	_, err = rdb.Ping(context.Background()).Result()
	if err != nil {
		w.logger.Fatal(err)
	}

	w.rdb = rdb

	ctx := context.Background()
	w.pubsub = w.rdb.Subscribe(ctx, w.channel)

	return w
}

// BeforeRun run script before start worker
func (s *Worker) BeforeRun() error {
	return nil
}

// AfterRun run script after start worker
func (s *Worker) AfterRun() error {
	return nil
}

func (s *Worker) handle(job queue.Job) error {
	// create channel with buffer size 1 to avoid goroutine leak
	done := make(chan error, 1)
	panicChan := make(chan interface{}, 1)
	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), job.Timeout)
	defer cancel()

	// run the job
	go func() {
		// handle panic issue
		defer func() {
			if p := recover(); p != nil {
				panicChan <- p
			}
		}()

		// run custom process function
		done <- s.runFunc(ctx, job)
	}()

	select {
	case p := <-panicChan:
		panic(p)
	case <-ctx.Done(): // timeout reached
		return ctx.Err()
	case <-s.stop: // shutdown service
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
func (s *Worker) Shutdown() error {
	if !atomic.CompareAndSwapInt32(&s.stopFlag, 0, 1) {
		return queue.ErrQueueShutdown
	}

	s.stopOnce.Do(func() {
		s.pubsub.Close()
		s.rdb.Close()
		close(s.stop)
	})
	return nil
}

// Capacity for channel
func (s *Worker) Capacity() int {
	return 0
}

// Usage for count of channel usage
func (s *Worker) Usage() int {
	return 0
}

// Queue send notification to queue
func (s *Worker) Queue(job queue.QueuedMessage) error {
	if atomic.LoadInt32(&s.stopFlag) == 1 {
		return queue.ErrQueueShutdown
	}

	ctx := context.Background()

	// Publish a message.
	err := s.rdb.Publish(ctx, s.channel, job.Bytes()).Err()
	if err != nil {
		return err
	}

	return nil
}

// Run start the worker
func (s *Worker) Run() error {
	// check queue status
	select {
	case <-s.stop:
		return queue.ErrQueueShutdown
	default:
	}

	var options []redis.ChannelOption
	ctx := context.Background()

	if s.channelSize > 1 {
		options = append(options, redis.WithChannelSize(s.channelSize))
	}

	ch := s.pubsub.Channel(options...)
	// make sure the connection is successful
	err := s.pubsub.Ping(ctx)
	if err != nil {
		return err
	}

	for {
		select {
		case m, ok := <-ch:
			if !ok {
				return fmt.Errorf("redis pubsub: channel=%s closed", s.channel)
			}

			var data queue.Job
			if err := json.Unmarshal([]byte(m.Payload), &data); err != nil {
				s.logger.Error("json unmarshal error: ", err)
				continue
			}
			if err := s.handle(data); err != nil {
				s.logger.Error("handle job error: ", err)
			}
		case <-s.stop:
			return nil
		}
	}
}
