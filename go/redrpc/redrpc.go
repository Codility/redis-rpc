package redrpc

import (
	"fmt"
	"time"
)

func callQueueName(prefix, funcName string) string {
	return fmt.Sprintf("%s:%s:calls", prefix, funcName)
}

func responseQueueName(prefix, funcName, reqId string) string {
	return fmt.Sprintf("%s:%s:result:%s", prefix, funcName, reqId)
}

func timestamp() string {
	return time.Now().Format(time.RFC3339)
}
