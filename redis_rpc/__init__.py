import json
import logging
import signal
from datetime import datetime
from uuid import uuid4


BLPOP_TIMEOUT = 1  # seconds


class RPCTimeout(Exception):
    pass


class RemoteException(Exception):
    pass


class RedisRPC:
    def __init__(self, redis, prefix='redis_rpc'):
        self._redis = redis
        self._prefix = prefix

    def serve(self, func_map):
        self._quit = False
        signal.signal(signal.SIGTERM, self.termination_signal)
        signal.signal(signal.SIGINT, self.termination_signal)

        queue_map = {self.call_queue_name(func_name): (func_name, func)
                     for (func_name, func) in func_map.items()}
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
        self._redis.rpush(self.response_queue_name(func_name, req_id), json.dumps(msg))

    def call(self, func_name, **kwargs):
        req_id = str(uuid4())
        msg = {'id': req_id,
               'ts': datetime.now().isoformat()}
        msg['kw'] = kwargs
        self._redis.rpush(self.call_queue_name(func_name), json.dumps(msg))

        # TODO: wait longer than BLPOP_TIMEOUT
        qn = self.response_queue_name(func_name, req_id)
        popped = self._redis.blpop([qn], BLPOP_TIMEOUT)
        if popped is None:
            raise RPCTimeout()

        (_, res_bytes) = popped
        res = json.loads(res_bytes)

        if res.get('exc'):
            raise RemoteException(res['exc'])

        return res.get('res')

    def call_queue_name(self, func_name):
        return ('%s:%s:calls' % (self._prefix, func_name)).encode('utf-8')

    def response_queue_name(self, func_name, req_id):
        return ('%s:%s:result:%s' % (self._prefix, func_name, req_id)).encode('utf-8')

    def termination_signal(self, signum, frame):
        logging.info('Received %s, will quit.', signal.Signals(signum).name)
        self._quit = True
