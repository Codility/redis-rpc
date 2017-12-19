package redrpc

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stvp/assert"
)

const BaseTestRedisPort = 7300

func testRedisAddr() string {
	return fmt.Sprintf("localhost:%d", BaseTestRedisPort)
}

func TestBaseUsage(t *testing.T) {
	redCmd := mustStartRedisServer(BaseTestRedisPort)
	defer redCmd.Process.Kill()

	red := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	cli := NewClient(red)

	_, err := cli.Call("get", map[string]interface{}{
		"k": "k0",
	})
	assert.True(t, IsRPCTimeout(err))

	_, err = cli.Call("unknown-command", map[string]interface{}{})
	assert.True(t, IsRPCTimeout(err))

	data := map[string]interface{}{}

	srv := NewServer(red, map[string]Handler{
		"get": HandlerFunc(func(req Request) (interface{}, error) {
			// raise error on non-existant keys to verify
			// propagation
			v, ok := data[req.GetString("k")]
			if !ok {
				return nil, fmt.Errorf("fake error")
			}
			return v, nil
		}),
		"set": HandlerFunc(func(req Request) (interface{}, error) {
			data[req.GetString("k")] = req.GetValue("v")
			return nil, nil
		}),
	})
	go srv.Run()
	defer srv.Close()

	res, err := cli.Call("set", map[string]interface{}{"k": "k1", "v": 123})
	assert.Nil(t, res)
	assert.Nil(t, err)

	res, err = cli.Call("get", map[string]interface{}{"k": "k1"})
	assert.Equal(t, res, 123.0)
	assert.Nil(t, err)

	res, err = cli.Call("get", map[string]interface{}{"k": "unknown-key"})
	assert.Nil(t, res)
	assert.True(t, IsRemoteException(err))

	res, err = cli.Call("get", map[string]interface{}{"unknown_arg": "some-value"})
	assert.Nil(t, res)
	assert.True(t, IsRemoteException(err))
}

func mustStartRedisServer(port int, args ...string) *exec.Cmd {
	fullArgs := append([]string{"--port", strconv.Itoa(port)}, args...)
	p := exec.Command("redis-server", fullArgs...)
	p.Stdout = os.Stdout
	p.Stderr = os.Stderr
	if err := p.Start(); err != nil {
		panic(err)
	}

	red := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	defer red.Close()

	for {
		_, err := red.Ping().Result()
		if err == nil {
			break
		} else {
			log.Print("%v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	return p
}
