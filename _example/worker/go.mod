module example

go 1.22

require (
	github.com/golang-queue/queue v0.3.0
	github.com/golang-queue/redisdb v0.0.0-00010101000000-000000000000
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/redis/go-redis/v9 v9.7.0 // indirect
	github.com/yassinebenaid/godump v0.11.1 // indirect
)

replace github.com/golang-queue/redisdb => ../../
