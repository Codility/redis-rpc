#!env python3

from redis import StrictRedis
from redis_rpc import Server


VALUES = {}


def rpc_set(k, v):
    VALUES[k] = v


def rpc_get(k):
    return VALUES[k]


def main():
    redis = StrictRedis.from_url("redis://localhost:6379/0")
    srv = Server(redis, {'set': rpc_set, 'get': rpc_get}, 'rpc_example')
    srv.serve()


if __name__ == '__main__':
    main()
