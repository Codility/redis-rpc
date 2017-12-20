package redrpc

import (
	"time"

	"github.com/go-redis/redis"
)

//////////////////////////////////////////////////
// DbAdapter

type DbAdapter interface {
	BLPop(timeout time.Duration, keys ...string) ([]string, error)
	RPushEx(key, value string, ttl time.Duration) error
}

//////////////////////////////////////////////////
// RedisAdapter

type RedisAdapter struct {
	red *redis.Client
}

func (a *RedisAdapter) BLPop(timeout time.Duration, keys ...string) ([]string, error) {
	return a.red.BLPop(time.Second, keys...).Result()
}

func (a *RedisAdapter) RPushEx(key, value string, ttl time.Duration) error {
	pipe := a.red.Pipeline()
	pipe.RPush(key, value)
	pipe.Expire(key, ttl)
	_, err := pipe.Exec()
	return err
}
