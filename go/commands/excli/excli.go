package main

import (
	"encoding/json"
	"fmt"
	"os"

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

func printUsage() {
	fmt.Printf("Usage: %s (get|set) k [v]\n", os.Args[0])
}

func main() {
	red := redis.NewClient(&redis.Options{
		Addr: REDIS_URI,
	})

	cli := redrpc.NewClient(red, &redrpc.Options{
		Prefix: "rpc_example",
	})
	args := os.Args
	if len(args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch args[1] {
	case "get":
		if len(args) != 3 {
			fmt.Printf("Usage: %s get k \n", os.Args[0])
			os.Exit(1)
		}
		k := args[2]
		resp, err := cli.Call("get", map[string]interface{}{"k": k})
		if err != nil {
			panic(err)
		}
		fmt.Println(resp)
	case "set":
		if len(args) != 4 {
			fmt.Printf("Usage: %s set k v \n", os.Args[0])
			os.Exit(1)
		}
		k := args[2]
		vs := args[3]

		var v interface{}
		err := json.Unmarshal([]byte(vs), &v)
		if err != nil {
			panic(err)
		}

		_, err = cli.Call("set", map[string]interface{}{"k": k, "v": v})
		if err != nil {
			panic(err)
		}
	}
}
