import asyncio
from collections.abc import Iterable
from functools import partial

import zmq

from aiozmq import create_zmq_connection

from .base import (
    NotFoundError,
    ParametersError,
    Service,
    ServiceClosedError,
    _BaseProtocol,
    _BaseServerProtocol,
)
from .log import logger


async def connect_pubsub(*, connect=None, bind=None, loop=None, translation_table=None):
    """A coroutine that creates and connects/binds pubsub client.

    Usually for this function you need to use connect parameter, but
    ZeroMQ does not forbid to use bind.

    translation_table -- an optional table for custom value translators.

    loop -- an optional parameter to point ZmqEventLoop.  If loop is
            None then default event loop will be given by
            asyncio.get_event_loop() call.

    Returns PubSubClient instance.

    """
    pass


async def serve_pubsub(
    handler,
    *,
    subscribe=None,
    connect=None,
    bind=None,
    loop=None,
    translation_table=None,
    log_exceptions=False,
    exclude_log_exceptions=(),
    timeout=None
):
    """A coroutine that creates and connects/binds pubsub server instance.

    Usually for this function you need to use *bind* parameter, but
    ZeroMQ does not forbid to use *connect*.

    handler -- an object which processes incoming pipeline calls.
               Usually you like to pass AttrHandler instance.

    log_exceptions -- log exceptions from remote calls if True.

    subscribe -- subscription specification.  Subscribe server to
                 topics.  Allowed parameters are str, bytes, iterable
                 of str or bytes.

    translation_table -- an optional table for custom value translators.

    exclude_log_exceptions -- sequence of exception classes than should not
                              be logged.

    timeout -- timeout for performing handling of async server calls.

    loop -- an optional parameter to point ZmqEventLoop.  If loop is
            None then default event loop will be given by
            asyncio.get_event_loop() call.

    Returns PubSubService instance.
    Raises OSError on system error.
    Raises TypeError if arguments have inappropriate type.

    """
    pass


class _ClientProtocol(_BaseProtocol):
    def call(self, topic, name, args, kwargs):
        pass


class PubSubClient(Service):
    def __init__(self, loop, proto):
        super().__init__(loop, proto)

    def publish(self, topic):
        """Return object for dynamic PubSub calls.

        The usage is:
        await client.publish('my_topic').ns.func(1, 2)

        topic argument may be None otherwise must be isntance of str or bytes
        """
        pass


class PubSubService(Service):
    def subscribe(self, topic):
        """Subscribe to the topic.

        topic argument must be str or bytes.
        Raises TypeError in other cases
        """
        pass

    def unsubscribe(self, topic):
        """Unsubscribe from the topic.

        topic argument must be str or bytes.
        Raises TypeError in other cases
        """
        pass


class _MethodCall:

    __slots__ = ("_proto", "_topic", "_names")

    def __init__(self, proto, topic, names=()):
        self._proto = proto
        self._topic = topic
        self._names = names

    def __getattr__(self, name):
        return self.__class__(self._proto, self._topic, self._names + (name,))

    def __call__(self, *args, **kwargs):
        if not self._names:
            raise ValueError("PubSub method name is empty")
        return self._proto.call(self._topic, ".".join(self._names), args, kwargs)


class _ServerProtocol(_BaseServerProtocol):
    def msg_received(self, data):
        pass

    def process_call_result(self, fut, *, name, args, kwargs):
        pass
