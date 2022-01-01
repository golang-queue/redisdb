package redisdb

import (
	"context"

	"github.com/golang-queue/queue"
)

// Option for queue system
type Option func(*options)

type options struct {
	runFunc          func(context.Context, queue.QueuedMessage) error
	logger           queue.Logger
	metric           queue.Metric
	addr             string
	db               int
	connectionString string
	password         string
	channelName      string
	channelSize      int
	cluster          bool
}

// WithAddr setup the addr of redis
func WithAddr(addr string) Option {
	return func(w *options) {
		w.addr = addr
	}
}

// WithPassword redis password
func WithDB(db int) Option {
	return func(w *options) {
		w.db = db
	}
}

// WithCluster redis cluster
func WithCluster(enable bool) Option {
	return func(w *options) {
		w.cluster = enable
	}
}

// WithChannelSize redis channel size
func WithChannelSize(size int) Option {
	return func(w *options) {
		w.channelSize = size
	}
}

// WithPassword redis password
func WithPassword(passwd string) Option {
	return func(w *options) {
		w.password = passwd
	}
}

// WithConnectionString redis connection string
func WithConnectionString(connectionString string) Option {
	return func(w *options) {
		w.connectionString = connectionString
	}
}

// WithChannel setup the channel of redis
func WithChannel(channel string) Option {
	return func(w *options) {
		w.channelName = channel
	}
}

// WithRunFunc setup the run func of queue
func WithRunFunc(fn func(context.Context, queue.QueuedMessage) error) Option {
	return func(w *options) {
		w.runFunc = fn
	}
}

// WithLogger set custom logger
func WithLogger(l queue.Logger) Option {
	return func(w *options) {
		w.logger = l
	}
}

// WithMetric set custom Metric
func WithMetric(m queue.Metric) Option {
	return func(w *options) {
		w.metric = m
	}
}

func newOptions(opts ...Option) options {
	defaultOpts := options{
		addr:        "127.0.0.1:6379",
		channelName: "queue",
		channelSize: 1024,
		logger:      queue.NewLogger(),
		runFunc: func(context.Context, queue.QueuedMessage) error {
			return nil
		},
		metric: queue.NewMetric(),
	}

	// Loop through each option
	for _, opt := range opts {
		// Call the option giving the instantiated
		opt(&defaultOpts)
	}

	return defaultOpts
}
