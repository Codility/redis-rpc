Experiment: proof-of-concept minimal RPC-over-Redis
===================================================

Goals:

- simple
- potentially cross-language
- focus on low latency rather than guaranteed execution
- multiple clients, multiple workers


Planned use
-----------

server:

    from redis_rpc import RedisRPC

    def func1(arg1, arg2):
        return arg1 + arg2

    redis = StrictRedis.from_url(...)
    rpc = RedisRPC(redis, prefix)
    rpc.serve({'func1': func1, 'func2': func2, ...})


client:

    from redis_rpc import RedisRPC

    redis = StrictRedis.from_url(...)
    rpc = RedisRPC(redis, prefix)
    print(rpc.call('func1', arg1=1, arg2=2))


Protocol
--------

JSON.

Expose functions as Redis queues, send back results via queues so that
clients can BLPOP and receive them immediately.

Every function is a separate Redis queue.  Shuffle queues in BLPOP to
avoid starvation.

Queues:

- "{prefix}:{func}:calls"
- "{prefix}:{func}:result:{call-id}"

Call message:

    {"id": "{uuid}",  # random, client-assigned
     "ts": "{time-stamp-iso8601}",
     "kw": {"arg1": "value1",
            "arg2": ["some", "list", "of", "values"]}
    }

Call is sent to a queue specific to a particular function, so there is
no need to additionally put function name in the message itself.

Arguments and results may be any JSON-encodable values, including
null/none.  This is not meant to be a framework that completely
ensures interoperability between clients and servers, it's up to the
author of request handlers to make sure exposed API will be usable for
planned clients.

Result message:

    # if successfull
    {"ts": "{time-stamp-iso8601}",
     "res": {result-value}}

    # or, if finished with an exception
    {"ts": "{time-stamp-iso8601}",
     "exc": {exception-string}}


Potential future
----------------

Multiple result values, with the API to receive them as soon as
available (in Python: iterator that blocks waiting for next value or
terminator).

Bi-directional communication with the worker.

Reject outdated calls.  Modify request format to make it cheap,
without having to deserialize the whole object.
