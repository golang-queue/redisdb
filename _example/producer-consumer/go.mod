module example

go 1.25.0

require (
	github.com/appleboy/graceful v1.3.0
	github.com/golang-queue/queue v0.5.0
	github.com/golang-queue/redisdb v0.4.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/redis/go-redis/v9 v9.19.0 // indirect
	github.com/yassinebenaid/godump v0.11.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

replace github.com/golang-queue/redisdb => ../../
