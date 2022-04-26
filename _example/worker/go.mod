module example

go 1.16

require (
	github.com/golang-queue/queue v0.1.0
	github.com/golang-queue/redisdb v0.0.4
)

replace github.com/golang-queue/redisdb => ../../
