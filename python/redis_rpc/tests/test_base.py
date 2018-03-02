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
        rpc = Server(redis, func_map, **kwargs)
        rpc.serve()

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

        with pytest.raises(RemoteException) as excinfo:
            cli.call('get', k='unknown-key')

        assert "KeyError: 'unknown-key'" in str(excinfo.value)

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


def test_server_rotates_queues():
    funcs = {name: lambda: None
             for name in ['a', 'b', 'c']}
    mockredis = Mock()
    mockredis.blpop.return_value = None

    def last_call_queues():
        return list(mockredis.blpop.call_args[0][0])

    srv = Server(mockredis, funcs)
    srv.serve_one()
    assert last_call_queues() == [b'redis_rpc:a:calls',
                                  b'redis_rpc:b:calls',
                                  b'redis_rpc:c:calls']
    srv.serve_one()
    assert last_call_queues() == [b'redis_rpc:b:calls',
                                  b'redis_rpc:c:calls',
                                  b'redis_rpc:a:calls']
    srv.serve_one()
    assert last_call_queues() == [b'redis_rpc:c:calls',
                                  b'redis_rpc:a:calls',
                                  b'redis_rpc:b:calls']
    srv.serve_one()
    assert last_call_queues() == [b'redis_rpc:a:calls',
                                  b'redis_rpc:b:calls',
                                  b'redis_rpc:c:calls']

def test_server_limit():
    n_calls = 0

    def inc():
        nonlocal n_calls
        n_calls += 1

    mockredis = Mock()
    mockredis.blpop.return_value = b'redis_rpc:inc:calls', b'{"id": "xxx"}'

    srv = Server(mockredis, {'inc': inc}, limit=42)
    srv.serve()
    assert n_calls == 42


def test_client_timeout():
    mockredis = Mock()

    mockredis.blpop.return_value = None

    cli = Client(mockredis, response_timeout=1)
    with pytest.raises(RPCTimeout):
        cli.call('fake_func')
    assert mockredis.blpop.call_count > 1


def test_override_response_timeout(redisdb):
    t1 = 0.5
    t2 = 1.5
    t3 = 3.0

    with rpc_server(redisdb, {'sleep': lambda t: time.sleep(t)}):
        cli = Client(redisdb, response_timeout=t1)
        with pytest.raises(RPCTimeout):
            cli.call('sleep', t=t2)
        cli.call('sleep', t=t2, response_timeout=t3)


def test_heartbeat(redisdb):
    cli = Client(redisdb)
    with rpc_server(
        redisdb,
        {'f': lambda x: x},
        name='X', id='42',
        heartbeat_period=0.5,
        heartbeat_expire=1
    ):
        time.sleep(0.2)  # Wait until heartbeat starts
        assert cli.is_server_online('X')
        assert cli.is_server_online('X', '42')
        assert not cli.is_server_online('X', '43')
        assert cli.get_online_servers('X') == ['42']
        time.sleep(1)
        assert cli.is_server_online('X')
    time.sleep(1.5)
    assert not cli.is_server_online('X')
    assert cli.get_online_servers('X') == []
