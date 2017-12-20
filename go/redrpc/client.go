package redrpc

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis"
	uuid "github.com/satori/go.uuid" // explicit name because goimports doesn't handle non-letters well
)

type Client struct {
	red  *redis.Client
	opts *Options
}

func NewClient(red *redis.Client, opts *Options) *Client {
	cli := &Client{
		red:  red,
		opts: OptsWithDefaults(opts),
	}
	return cli
}

func (c *Client) CallAsync(funcName string, kwargs map[string]interface{}) (string, error) {
	reqId := uuid.NewV4()
	msg := map[string]interface{}{
		"id": reqId,
		"ts": timestamp(),
		"kw": kwargs,
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return "", err
	}

	err = rpushEx(c.red, callQueueName(c.opts.Prefix, funcName), string(msgBytes), c.opts.RequestExpire)
	if err != nil {
		return "", err
	}

	return reqId.String(), nil
}

func (c *Client) Call(funcName string, kwargs map[string]interface{}) (interface{}, error) {
	reqId, err := c.CallAsync(funcName, kwargs)
	if err != nil {
		return nil, err
	}
	return c.Response(funcName, reqId)
}

func (c *Client) Response(funcName, reqId string) (interface{}, error) {
	startTs := time.Now()
	deadlineTs := startTs.Add(c.opts.ResponseTimeout)
	queueName := responseQueueName(c.opts.Prefix, funcName, reqId)

	for {
		nowTs := time.Now()
		if nowTs.After(deadlineTs) {
			return nil, &RPCTimeout{}
		}

		waitTime := deadlineTs.Sub(nowTs)
		if c.opts.BLPOPTimeout < waitTime {
			waitTime = c.opts.BLPOPTimeout
		}
		if waitTime < time.Second {
			waitTime = time.Second
		}

		res, err := c.red.BLPop(waitTime, queueName).Result()
		if err == redis.Nil {
			// nothing showed up
			continue
		}
		if err != nil {
			log.Print("Client.Response got an error in BLPOP: ", err)
			return nil, err
		}

		var data map[string]interface{}
		if err := json.Unmarshal([]byte(res[1]), &data); err != nil {
			log.Print("Client.Response got a malformed message: ", res[1])
			continue
		}
		if remoteErr, ok := data["err"]; ok {
			return nil, &RemoteException{fmt.Sprint(remoteErr)}
		}
		return data["res"], nil
	}
}
