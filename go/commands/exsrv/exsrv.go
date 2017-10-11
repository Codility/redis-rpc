package main

import (
	"github.com/go-redis/redis"
	"github.com/marcinkaszynski/redis_rpc/go/redrpc"
)

var data = map[string]interface{}{}

func get(req redrpc.Request) (interface{}, error) {
	return data[req.GetString("k")], nil
}

func set(req redrpc.Request) (interface{}, error) {
	data[req.GetString("k")] = req.GetValue("v")
	return nil, nil
}

func main() {
	red := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	srv := redrpc.NewServer(red, map[string]redrpc.Handler{
		"get": redrpc.HandlerFunc(get),
		"set": redrpc.HandlerFunc(set),
	})
	srv.Run()
}
