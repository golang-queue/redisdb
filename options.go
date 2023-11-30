package redisdb

import (
	"context"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
)

// Option for queue system
type Option func(*options)

type options struct {
	runFunc          func(context.Context, core.QueuedMessage) error
	logger           queue.Logger
	addr             string
	db               int
	connectionString string
	password         string
	channelName      string
	channelSize      int
	cluster          bool
	sentinel         bool
	masterName       string
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

// WithSentinel redis sentinel
func WithSentinel(enable bool) Option {
	return func(w *options) {
		w.sentinel = enable
	}
}

// WithMasterName sentinel master name
func WithMasterName(masterName string) Option {
	return func(w *options) {
		w.masterName = masterName
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
func WithRunFunc(fn func(context.Context, core.QueuedMessage) error) Option {
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

func newOptions(opts ...Option) options {
	defaultOpts := options{
		addr:        "127.0.0.1:6379",
		channelName: "queue",
		// default channel size in go-redis package
		channelSize: 100,
		logger:      queue.NewLogger(),
		runFunc: func(context.Context, core.QueuedMessage) error {
			return nil
		},
	}

	// Loop through each option
	for _, opt := range opts {
		// Call the option giving the instantiated
		opt(&defaultOpts)
	}

	return defaultOpts
}
