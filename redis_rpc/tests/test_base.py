import pytest
import time
from contextlib import contextmanager
from multiprocessing import Process
from redis_rpc import (Client, Server, RemoteException, RPCTimeout,
                       call_queue_name, response_queue_name)
from unittest.mock import Mock


@contextmanager
def rpc_server(redis, func_map, **kwargs):

    def server():
        rpc = Server(redis, **kwargs)
        rpc.serve(func_map)

    rpc_proc = Process(target=server)
    rpc_proc.start()

    try:
        yield rpc_proc
    finally:
        rpc_proc.terminate()
        rpc_proc.join()


def test_base_usage(redisdb):
    cli = Client(redisdb)

    with pytest.raises(RPCTimeout):
        cli.call('get', k='k0')

    with pytest.raises(RPCTimeout):
        cli.call('unknown-function')

    data = {}

    def fget(k):
        return data[k]

    def fset(k, v):
        data[k] = v

    funcs = {'get': fget, 'set': fset}

    with rpc_server(redisdb, funcs):
        assert cli.call('set', k='k1', v=123) is None
        assert cli.call('get', k='k1') == 123

        assert cli.call('set', k='k2', v=None) is None
        assert cli.call('get', k='k2') is None

        with pytest.raises(RemoteException):
            cli.call('get', k='unknown-key')

        with pytest.raises(RemoteException):
            cli.call('get', unknown_arg='some-value')


def test_expiry_times(redisdb):
    cli = Client(redisdb, request_expire=10)

    req_id = cli.call_async('zero')
    assert 0 < redisdb.ttl(call_queue_name('redis_rpc', 'zero')) <= 10

    resp_queue = response_queue_name('redis_rpc', 'zero', req_id)
    with rpc_server(redisdb, {'zero': lambda: 0}, result_expire=10):
        while redisdb.ttl(resp_queue) <= 0:
            time.sleep(0.1)
        assert 0 < redisdb.ttl(resp_queue) <= 10
