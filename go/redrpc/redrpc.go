package redrpc

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

const (
	BLPOPTimeout    = 1 * time.Second
	ResponseTimeout = 1 * time.Second

	RequestExpire = 120 * time.Second
	ResultExpire  = 120 * time.Second
)

func callQueueName(prefix, funcName string) string {
	return fmt.Sprintf("%s:%s:calls", prefix, funcName)
}

func responseQueueName(prefix, funcName, reqId string) string {
	return fmt.Sprintf("%s:%s:result:%s", prefix, funcName, reqId)
}

func rpushEx(red *redis.Client, key, value string, ttl time.Duration) error {
	pipe := red.Pipeline()
	pipe.RPush(key, value)
	pipe.Expire(key, ttl)
	_, err := pipe.Exec()
	return err
}

func timestamp() string {
	return time.Now().Format(time.RFC3339)
}
