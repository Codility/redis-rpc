#!env python3

import sys
from redis import StrictRedis
from redis_rpc import Client


def main():
    redis = StrictRedis.from_url("redis://localhost:6379/0", socket_timeout=10)
    cli = Client(redis, 'rpc_example')

    if sys.argv[1] == 'get':
        print(cli.call('get', k=sys.argv[2]))
    elif sys.argv[1] == 'set':
        print(cli.call('set', k=sys.argv[2], v=sys.argv[3]))
    else:
        print("Unknown command:", sys.argv[1])


if __name__ == '__main__':
    main()
