import re
import sys
import json
import logging
import math
import signal
import time
import threading
import traceback
from datetime import datetime
from uuid import uuid4


log = logging.getLogger('redis-rpc')


# All timeouts and expiry times are in seconds
BLPOP_TIMEOUT = 1
RESPONSE_TIMEOUT = 1
REQUEST_EXPIRE = 120
RESULT_EXPIRE = 120
HEARTBEAT_PERIOD = 3
HEARTBEAT_EXPIRE = 5


class RPCTimeout(Exception):
    pass


class RemoteException(Exception):
    def __init__(self, message, traceback=None):
        self.message = message
        self.traceback = traceback

    def __str__(self):
        s = self.message
        if self.traceback:
            s += '\n\nRemote traceback:\n' + self.traceback
        return s


def call_queue_name(prefix, func_name):
    return ('%s:%s:calls' % (prefix, func_name)).encode('utf-8')


def response_queue_name(prefix, func_name, req_id):
    return ('%s:%s:result:%s' % (prefix, func_name, req_id)).encode('utf-8')


def rotated(l, places):
    places = places % len(l)
    return l[places:] + l[:places]


def warn_if_no_socket_timeout(redis):
    if redis.connection_pool.connection_kwargs.get('socket_timeout') is None:
        log.warning('RPC: Redis instance does not set socket_timeout.  '
                    'This means potential trouble in case of network '
                    'problems between Redis and RPC client or server.')


def format_for_logs(s, *, verbose):
    if s is None:
        return '-'
    if isinstance(s, bytes):
        s = s.decode()
    if not verbose and len(s) > 120:
        return s[:120] + '...'
    # quick and dirty way to avoid log forging/injection
    return json.dumps(s)


def format_exception_for_logs(exception, *, verbose):
    if exception is None:
        return '-'
    return format_for_logs('%s: %s' % (type(exception).__name__,
                                       str(exception)), verbose=verbose)


def log_request(func_name, req_bytes, exception, result, msg, *, verbose):
    parts = [
        func_name,
        format_for_logs(req_bytes, verbose=verbose),
        format_exception_for_logs(exception, verbose=verbose),
        format_for_logs(result, verbose=verbose),
        msg,
    ]

    fmt = ' '.join(['%s']*len(parts))
    if exception:
        log.exception(fmt, *parts)
    else:
        log.info(fmt, *parts)


# Atomic RPUSH + EXPIRE.
# (The pipeline is executed as MULTI by redis-py).
def rpush_ex(redis, key, value, ttl):
    pipe = redis.pipeline()
    pipe.rpush(key, value)
    pipe.expire(key, ttl)
    pipe.execute()


class Client:
    def __init__(self, redis,
                 name='',
                 id='',
                 prefix='redis_rpc',
                 request_expire=REQUEST_EXPIRE,
                 blpop_timeout=BLPOP_TIMEOUT,
                 response_timeout=RESPONSE_TIMEOUT):
        self.name = name
        self.id = id
        self._redis = redis
        self._prefix = prefix
        self._expire = request_expire
        self._blpop_timeout = blpop_timeout
        self._response_timeout = response_timeout
        warn_if_no_socket_timeout(redis)

    def call_async(self, func_name, **kwargs):
        req_id = str(uuid4())
        msg = {'id': req_id,
               'ts': datetime.now().isoformat()}
        msg['kw'] = kwargs

        rpush_ex(self._redis,
                 call_queue_name(self._prefix, func_name),
                 json.dumps(msg).encode(),
                 self._expire)

        return req_id

    def response(self, func_name, req_id, *, response_timeout=None):
        start_ts = time.time()
        if response_timeout is None:
            response_timeout = self._response_timeout
        deadline_ts = start_ts + response_timeout

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
            raise RemoteException(res['err'], res.get('err_traceback'))
        return res.get('res')

    def call(self, func_name, *, response_timeout=None, **kwargs):
        req_id = self.call_async(func_name, **kwargs)
        return self.response(func_name, req_id,
                             response_timeout=response_timeout)

    def is_online(self, server_id=None):
        if server_id is not None:
            key = '{}:{}:{}:alive'.format(self.prefix, self.name, server_id)
            if self._redis.get(key):
                return True
            return False
        pattern = '{}:{}:*:alive'.format(self.prefix, self.name)
        for server in self._redis.scan_iter(match=pattern):
            return True
        return False


class Server:
    def __init__(self, redis, func_map,
                 name='',
                 id='',
                 prefix='redis_rpc',
                 result_expire=RESULT_EXPIRE,
                 blpop_timeout=BLPOP_TIMEOUT,
                 heartbeat_period=HEARTBEAT_PERIOD,
                 heartbeat_expire=HEARTBEAT_EXPIRE,
                 verbose=False,
                 limit=None):
        self.name = name
        self.id = id
        self._redis = redis
        self._prefix = prefix
        self._expire = result_expire
        self._blpop_timeout = blpop_timeout
        self._func_map = func_map
        self._heartbeat_period = heartbeat_period
        self._heartbeat_expire = heartbeat_expire
        self._queue_map = {call_queue_name(self._prefix, name): (name, func)
                           for (name, func) in func_map.items()}
        self._queue_names = sorted((self._queue_map.keys()))
        self._call_idx = 0
        self._quit = False
        self._verbose = False
        self._limit = limit
        self._served = 0
        warn_if_no_socket_timeout(redis)

    @property
    def queue_names(self):
        return list(self._queue_names)

    def serve(self, num_threads=1):
        def _serve():
            while not self._quit:
                self.serve_one()

        heartbeat_thread = threading.Thread(target=heartbeat)
        heartbeat_thread.start()
        if num_threads == 1:
            _serve()
        else:
            assert num_threads > 1
            threads = [threading.Thread(target=_serve) for i in range(num_threads)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        heartbeat_thread.join()

    def heartbeat(self):
        while not self._quit:
            self._redis.set('{}:{}:{}:alive'.format(
                self.prefix, self.name, self.id), True, ex=self._heartbeat_expire
            )
            time.sleep(self._heartbeat_period)

    def quit(self):
        self._quit = True

    def serve_one(self):
        popped = self._redis.blpop(rotated(self._queue_names, self._call_idx),
                                   self._blpop_timeout)
        self._call_idx += 1
        if popped is None:
            return

        (queue, req_bytes) = popped
        (func_name, func) = self._queue_map[queue]
        try:
            req = json.loads(req_bytes.decode())
        except Exception as e:
            log_request(func_name, req_bytes, e, None,
                        'Could not parse incoming message')
            return

        try:
            res = func(**req.get('kw', {}))
            self.send_result(func_name, req['id'], res=res,
                             verbose=self._verbose)
        except Exception as e:
            # TODO: format information about exception in a nicer way
            log_request(func_name, req_bytes, e, None,
                        'Caught exception while calling %s' % func_name,
                        verbose=self._verbose)
            err = '{}: {}'.format(type(e).__name__, str(e))
            err_traceback = re.sub(r'^.*\n', '', traceback.format_exc())
            self.send_result(func_name, req['id'],
                             err=err, err_traceback=err_traceback)
        else:
            log_request(func_name, req_bytes, None, json.dumps(res), 'OK',
                        verbose=self._verbose)

        self._served += 1
        if self._limit is not None and self._served >= self._limit:
            log.info('Served %d requests, will quit.', self._served)
            self._quit = True

    def send_result(self, func_name, req_id, **kwargs):
        msg = {'ts': datetime.now().isoformat()}
        msg.update(kwargs)
        rpush_ex(self._redis,
                 response_queue_name(self._prefix, func_name,
                                     req_id),
                 json.dumps(msg).encode(), self._expire)

    def quit_on_signals(self, signals=[signal.SIGTERM, signal.SIGINT]):
        for s in signals:
            signal.signal(s, self.termination_signal)

    def termination_signal(self, signum, frame):
        log.info('Received %s, will quit.', signal.Signals(signum).name)
        self.quit()
