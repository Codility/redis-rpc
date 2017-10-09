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
    rpc = Server(redis, 'rpc_example')
    rpc.serve({'set': rpc_set, 'get': rpc_get})


if __name__ == '__main__':
    main()
