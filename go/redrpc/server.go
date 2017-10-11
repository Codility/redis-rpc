package redrpc

import (
	"encoding/json"
	"log"
	"time"

	"github.com/go-redis/redis"
)

type Handler interface {
	ServeRPC(Request) (interface{}, error)
}

type HandlerFunc func(Request) (interface{}, error)

func (h HandlerFunc) ServeRPC(r Request) (interface{}, error) {
	return h(r)
}

type Request interface {
	GetValue(name string) interface{}
	GetString(name string) string
}

func callQueueName(prefix, func_name string) string {
	return prefix + ":" + func_name + ":calls"
}

func responseQueueName(prefix, func_name, req_id string) string {
	return prefix + ":" + func_name + ":result:" + req_id
}

type Server struct {
	red      *redis.Client
	handlers map[string]Handler
	prefix   string
	queues   []string
	queueMap map[string]string
}

type RequestImpl struct {
	Id string                 `json:"id"`
	Ts string                 `json:"ts"`
	Kw map[string]interface{} `json:"kw"`
}

func (r RequestImpl) GetValue(k string) interface{} {
	return r.Kw[k]
}

func (r RequestImpl) GetString(k string) string {
	return r.GetValue(k).(string)
}

// TODO: ServerOptions: prefix, timeouts, etc
func NewServer(red *redis.Client, handlers map[string]Handler) *Server {
	srv := &Server{
		red:      red,
		handlers: handlers,
		prefix:   "redis_rpc",
		queues:   []string{},
		queueMap: map[string]string{},
	}
	for func_name, _ := range handlers {
		qname := callQueueName(srv.prefix, func_name)
		srv.queues = append(srv.queues, qname)
		srv.queueMap[qname] = func_name
	}
	return srv
}

func (s *Server) Run() {
	for {
		res, err := s.red.BLPop(time.Second, s.queues...).Result()
		if err == redis.Nil {
			// queue doesn't exist
			continue
		}
		if err != nil {
			log.Print("Error in BLPOP: ", err)
			return
		}
		if len(res) > 1 {
			s.handleBLPopResult(res)
		}
	}
}

func (s *Server) handleBLPopResult(res []string) error {
	queue := res[0]
	msg := res[1]

	req := &RequestImpl{}
	err := json.Unmarshal([]byte(msg), req)
	if err != nil {
		log.Printf("Could not unmarshal request: %s", err)
		return err
	}

	func_name := s.queueMap[queue]
	handler := s.handlers[func_name]
	return s.callHandler(func_name, req, handler)
}

func (s *Server) callHandler(func_name string, req *RequestImpl, handler Handler) error {
	// TODO: recover
	res, err := handler.ServeRPC(req)
	if err != nil {
		s.sendResponse(func_name, req, ErrResponse{
			Ts:  "TODO",
			Err: err.Error(),
		})
	} else {
		s.sendResponse(func_name, req, ResResponse{
			Ts:  "TODO",
			Res: res,
		})
	}

	return nil
}

type ErrResponse struct {
	Ts  string `json:"ts"`
	Err string `json:"err"`
}

type ResResponse struct {
	Ts  string      `json:"ts"`
	Res interface{} `json:"res"`
}

func (s *Server) sendResponse(func_name string, req *RequestImpl, res interface{}) error {
	// TODO: expire

	msg, err := json.Marshal(res)
	if err != nil {
		log.Println("Error while marshaling reply: ", res)
		return err
	}

	qn := responseQueueName(s.prefix, func_name, req.Id)

	if _, err = s.red.RPush(qn, msg).Result(); err != nil {
		log.Println("Error while sending reply: ", err)
		return err
	}

	return nil
}
