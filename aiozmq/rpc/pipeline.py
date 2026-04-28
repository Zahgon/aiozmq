import asyncio
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
from .util import (
    _MethodCall,
)


async def connect_pipeline(
    *, connect=None, bind=None, loop=None, translation_table=None
):
    """A coroutine that creates and connects/binds Pipeline client instance.

    Usually for this function you need to use *connect* parameter, but
    ZeroMQ does not forbid to use *bind*.

    translation_table -- an optional table for custom value translators.

    loop --  an optional parameter to point
    ZmqEventLoop instance.  If loop is None then default
    event loop will be given by asyncio.get_event_loop() call.

    Returns PipelineClient instance.
    """
    pass


async def serve_pipeline(
    handler,
    *,
    connect=None,
    bind=None,
    loop=None,
    translation_table=None,
    log_exceptions=False,
    exclude_log_exceptions=(),
    timeout=None
):
    """A coroutine that creates and connects/binds Pipeline server instance.

    Usually for this function you need to use *bind* parameter, but
    ZeroMQ does not forbid to use *connect*.

    handler -- an object which processes incoming pipeline calls.
               Usually you like to pass AttrHandler instance.

    log_exceptions -- log exceptions from remote calls if True.

    translation_table -- an optional table for custom value translators.

    exclude_log_exceptions -- sequence of exception classes than should not
                              be logged.

    timeout -- timeout for performing handling of async server calls.

    loop -- an optional parameter to point ZmqEventLoop instance.  If
            loop is None then default event loop will be given by
            asyncio.get_event_loop() call.

    Returns Service instance.

    """
    pass


class _ClientProtocol(_BaseProtocol):
    def call(self, name, args, kwargs):
        pass


class PipelineClient(Service):
    def __init__(self, loop, proto):
        super().__init__(loop, proto)

    @property
    def notify(self):
        """Return object for dynamic Pipeline calls.

        The usage is:
        await client.pipeline.ns.func(1, 2)
        """
        pass


class _ServerProtocol(_BaseServerProtocol):
    def msg_received(self, data):
        pass

    def process_call_result(self, fut, *, name, args, kwargs):
        pass
