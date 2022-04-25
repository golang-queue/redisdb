module example

go 1.16

require (
	github.com/appleboy/graceful v0.0.4
	github.com/golang-queue/queue v0.0.13-0.20220423074840-a2e1b18a04ae
	github.com/golang-queue/redisdb v0.0.4
)

replace github.com/golang-queue/redisdb => ../../
