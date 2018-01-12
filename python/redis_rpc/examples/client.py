#!env python3

import json
import sys
from redis import StrictRedis
from redis_rpc import Client


USAGE = """Usage:

  client get <keyname>
  client set <keyname> <json-value>

Examples:
  client get mykey
  client set mykey 123
  client set mykey '"text"'
  client set mykey '{"complex": {"data": "structure"}}'
"""


def main():
    redis = StrictRedis.from_url("redis://localhost:6379/0", socket_timeout=10)
    cli = Client(redis, prefix='rpc_example')

    if sys.argv[1] == 'get':
        print(json.dumps(cli.call('get', k=sys.argv[2])))
    elif sys.argv[1] == 'set':
        print(cli.call('set', k=sys.argv[2], v=json.loads(sys.argv[3])))
    else:
        print(USAGE)


if __name__ == '__main__':
    main()
