import json
import logging
import signal
from datetime import datetime
from uuid import uuid4


BLPOP_TIMEOUT = 1  # seconds
REQUEST_EXPIRE = 120
RESULT_EXPIRE = 120


class RPCTimeout(Exception):
    pass


class RemoteException(Exception):
    pass


def call_queue_name(prefix, func_name):
    return ('%s:%s:calls' % (prefix, func_name)).encode('utf-8')


def response_queue_name(prefix, func_name, req_id):
    return ('%s:%s:result:%s' % (prefix, func_name, req_id)).encode('utf-8')


class Scripts:

    RPUSH_EX = ("redis.call('rpush', KEYS[1], ARGV[1]);"
                "redis.call('expire', KEYS[1], ARGV[2])")

    def __init__(self, redis):
        self.redis = redis
        self._rpush_ex = redis.register_script(self.RPUSH_EX)

    def rpush_ex(self, queue, msg, expire):
        self._rpush_ex(keys=[queue], args=[msg, expire])


class Client:
    def __init__(self, redis, prefix='redis_rpc',
                 request_expire=REQUEST_EXPIRE):
        self._redis = redis
        self._prefix = prefix
        self._scripts = Scripts(redis)
        self._expire = request_expire

    def call_async(self, func_name, **kwargs):
        req_id = str(uuid4())
        msg = {'id': req_id,
               'ts': datetime.now().isoformat()}
        msg['kw'] = kwargs

        self._scripts.rpush_ex(call_queue_name(self._prefix, func_name),
                               json.dumps(msg), self._expire)

        return req_id

    def response(self, func_name, req_id):
        # TODO: wait longer than BLPOP_TIMEOUT
        qn = response_queue_name(self._prefix, func_name, req_id)
        popped = self._redis.blpop([qn], BLPOP_TIMEOUT)
        if popped is None:
            raise RPCTimeout()

        (_, res_bytes) = popped
        res = json.loads(res_bytes)
        if res.get('exc'):
            raise RemoteException(res['exc'])
        return res.get('res')

    def call(self, func_name, **kwargs):
        req_id = self.call_async(func_name, **kwargs)
        return self.response(func_name, req_id)


class Server:
    def __init__(self, redis, prefix='redis_rpc', result_expire=RESULT_EXPIRE):
        self._redis = redis
        self._prefix = prefix
        self._scripts = Scripts(redis)
        self._expire = result_expire

    def serve(self, func_map):
        self._quit = False
        signal.signal(signal.SIGTERM, self.termination_signal)
        signal.signal(signal.SIGINT, self.termination_signal)

        queue_map = {call_queue_name(self._prefix, name): (name, func)
                     for (name, func) in func_map.items()}
        while not self._quit:
            self.serve_one(queue_map)

    def serve_one(self, queue_map):
        # TODO: rotate the order of queues in blpop to avoid starvation
        popped = self._redis.blpop(queue_map.keys(), BLPOP_TIMEOUT)
        if popped is None:
            return

        (queue, req_str) = popped
        (func_name, func) = queue_map[queue]
        try:
            req = json.loads(req_str)
        except Exception as e:
            logging.exception('Could not parse incoming message: %s', req_str)
            return

        try:
            res = func(**req.get('kw', {}))
            self.send_result(func_name, req['id'], res=res)
        except Exception as e:
            # TODO: format information about exception in a nicer way
            logging.exception('Caught exception while calling %s', func_name)
            self.send_result(func_name, req['id'], exc=repr(e))

    def send_result(self, func_name, req_id, **kwargs):
        msg = {'ts': datetime.now().isoformat()}
        msg.update(kwargs)
        self._scripts.rpush_ex(response_queue_name(self._prefix, func_name,
                                                   req_id),
                               json.dumps(msg), self._expire)

    def termination_signal(self, signum, frame):
        logging.info('Received %s, will quit.', signal.Signals(signum).name)
        self._quit = True
