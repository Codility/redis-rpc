import pytest
from contextlib import contextmanager
from multiprocessing import Process
from redis_rpc import RedisRPC, RemoteException, RPCTimeout


@contextmanager
def rpc_server(redis, func_map):

    def server():
        rpc = RedisRPC(redis)
        rpc.serve(func_map)

    rpc_proc = Process(target=server)
    rpc_proc.start()

    try:
        yield rpc_proc
    finally:
        rpc_proc.terminate()
        rpc_proc.join()


def test_client_server(redisdb):
    data = {}

    def fget(k):
        return data[k]

    def fset(k, v):
        data[k] = v

    cli = RedisRPC(redisdb)

    with pytest.raises(RPCTimeout):
        cli.call('get', k='k0')

    with pytest.raises(RPCTimeout):
        cli.call('unknown-function')

    with rpc_server(redisdb, {'get': fget, 'set': fset}):

        assert cli.call('set', k='k1', v=123) is None
        assert cli.call('get', k='k1') == 123

        assert cli.call('set', k='k2', v=None) is None
        assert cli.call('get', k='k2') is None

        with pytest.raises(RemoteException):
            cli.call('get', k='unknown-key')

        with pytest.raises(RemoteException):
            cli.call('get', unknown_arg='some-value')
