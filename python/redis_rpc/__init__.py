import json
import logging
import math
import signal
import time
from datetime import datetime
from uuid import uuid4


# All timeouts and expiry times are in seconds
BLPOP_TIMEOUT = 1
RESPONSE_TIMEOUT = 1
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


def rotated(l, places):
    places = places % len(l)
    return l[places:] + l[:places]


def warn_if_no_socket_timeout(redis):
    if redis.connection_pool.connection_kwargs.get('socket_timeout') is None:
        logging.warning('RPC: Redis instance does not set socket_timeout.  '
                        'This means potential trouble in case of network '
                        'problems between Redis and RPC client or server.')


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
                 request_expire=REQUEST_EXPIRE,
                 blpop_timeout=BLPOP_TIMEOUT,
                 response_timeout=RESPONSE_TIMEOUT):
        self._redis = redis
        self._prefix = prefix
        self._scripts = Scripts(redis)
        self._expire = request_expire
        self._blpop_timeout = blpop_timeout
        self._response_timeout = response_timeout
        warn_if_no_socket_timeout(redis)

    def call_async(self, func_name, **kwargs):
        req_id = str(uuid4())
        msg = {'id': req_id,
               'ts': datetime.now().isoformat()}
        msg['kw'] = kwargs

        self._scripts.rpush_ex(call_queue_name(self._prefix, func_name),
                               json.dumps(msg).encode(), self._expire)

        return req_id

    def response(self, func_name, req_id):
        start_ts = time.time()
        deadline_ts = start_ts + self._response_timeout

        qn = response_queue_name(self._prefix, func_name, req_id)

        popped = None
        while popped is None:
            now_ts = time.time()
            if now_ts >= deadline_ts:
                raise RPCTimeout()

            wait_time = math.ceil(min(self._blpop_timeout, deadline_ts - now_ts))
            popped = self._redis.blpop([qn], wait_time)

        (_, res_bytes) = popped
        res = json.loads(res_bytes.decode())
        if res.get('err'):
            raise RemoteException(res['err'])
        return res.get('res')

    def call(self, func_name, **kwargs):
        req_id = self.call_async(func_name, **kwargs)
        return self.response(func_name, req_id)


class Server:
    def __init__(self, redis, func_map,
                 prefix='redis_rpc',
                 result_expire=RESULT_EXPIRE,
                 blpop_timeout=BLPOP_TIMEOUT):
        self._redis = redis
        self._prefix = prefix
        self._scripts = Scripts(redis)
        self._expire = result_expire
        self._blpop_timeout = blpop_timeout
        self._func_map = func_map
        self._queue_map = {call_queue_name(self._prefix, name): (name, func)
                           for (name, func) in func_map.items()}
        self._queue_names = sorted((self._queue_map.keys()))
        self._call_idx = 0
        warn_if_no_socket_timeout(redis)

    def serve(self):
        self._quit = False
        signal.signal(signal.SIGTERM, self.termination_signal)
        signal.signal(signal.SIGINT, self.termination_signal)

        while not self._quit:
            self.serve_one()

    def serve_one(self):
        popped = self._redis.blpop(rotated(self._queue_names, self._call_idx),
                                   self._blpop_timeout)
        self._call_idx += 1
        if popped is None:
            return

        (queue, req_str) = popped
        (func_name, func) = self._queue_map[queue]
        try:
            req = json.loads(req_str.decode())
        except Exception as e:
            logging.exception('Could not parse incoming message: %s', req_str)
            return

        try:
            res = func(**req.get('kw', {}))
            self.send_result(func_name, req['id'], res=res)
        except Exception as e:
            # TODO: format information about exception in a nicer way
            logging.exception('Caught exception while calling %s', func_name)
            self.send_result(func_name, req['id'], err=repr(e))

    def send_result(self, func_name, req_id, **kwargs):
        msg = {'ts': datetime.now().isoformat()}
        msg.update(kwargs)
        self._scripts.rpush_ex(response_queue_name(self._prefix, func_name,
                                                   req_id),
                               json.dumps(msg).encode(), self._expire)

    def termination_signal(self, signum, frame):
        logging.info('Received %s, will quit.', signal.Signals(signum).name)
        self._quit = True
