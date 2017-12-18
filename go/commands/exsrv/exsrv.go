package main

import (
	"fmt"

	"github.com/Codility/redis-rpc/go/redrpc"
	"github.com/go-redis/redis"
)

var data = map[string]interface{}{}

func get(req redrpc.Request) (interface{}, error) {
	return data[req.GetString("k")], nil
}

func set(req redrpc.Request) (interface{}, error) {
	data[req.GetString("k")] = req.GetValue("v")
	return nil, nil
}

const REDIS_URI = "localhost:6379"

func main() {
	red := redis.NewClient(&redis.Options{
		Addr: REDIS_URI,
	})
	srv := redrpc.NewServer(red, map[string]redrpc.Handler{
		"get": redrpc.HandlerFunc(get),
		"set": redrpc.HandlerFunc(set),
	})
	fmt.Println("Starting srv")
	srv.Run()
}
