package redisdb

import (
	"context"
	"sync"
	"sync/atomic"

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
	addr             string
	db               int
	connectionString string
	password         string

	startOnce sync.Once
	stopOnce  sync.Once
	stop      chan struct{}
	channel   string
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
		addr:    "127.0.0.1:6379",
		channel: "queue",
		stop:    make(chan struct{}),
		logger:  queue.NewLogger(),
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
	return nil
}

// Shutdown worker
func (s *Worker) Shutdown() error {
	if !atomic.CompareAndSwapInt32(&s.stopFlag, 0, 1) {
		return queue.ErrQueueShutdown
	}

	s.stopOnce.Do(func() {
		if atomic.LoadInt32(&s.startFlag) == 1 {
			s.rdb.Close()
		}

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

	return nil
}

// Run start the worker
func (s *Worker) Run() error {
	return nil
}
