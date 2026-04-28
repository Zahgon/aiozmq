"""ZeroMQ RPC"""
import asyncio
import os
import random
import struct
import sys
import time
from collections import ChainMap
from functools import partial

import zmq
from aiozmq import create_zmq_connection

from .base import (
    GenericError,
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
    _fill_error_table,
)


__all__ = [
    "connect_rpc",
    "serve_rpc",
]


async def connect_rpc(
    *,
    connect=None,
    bind=None,
    loop=None,
    error_table=None,
    translation_table=None,
    timeout=None
):
    """A coroutine that creates and connects/binds RPC client.

    Usually for this function you need to use *connect* parameter, but
    ZeroMQ does not forbid to use *bind*.

    error_table -- an optional table for custom exception translators.

    timeout -- an optional timeout for RPC calls. If timeout is not
    None and remote call takes longer than timeout seconds then
    asyncio.TimeoutError will be raised at client side. If the server
    will return an answer after timeout has been raised that answer
    **is ignored**.

    translation_table -- an optional table for custom value translators.

    loop -- an optional parameter to point ZmqEventLoop instance.  If
    loop is None then default event loop will be given by
    asyncio.get_event_loop call.

    Returns a RPCClient instance.
    """
    pass


async def serve_rpc(
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
    """A coroutine that creates and connects/binds RPC server instance.

    Usually for this function you need to use *bind* parameter, but
    ZeroMQ does not forbid to use *connect*.

    handler -- an object which processes incoming RPC calls.  Usually
               you like to pass AttrHandler instance.

    log_exceptions -- log exceptions from remote calls if True.

    exclude_log_exceptions -- sequence of exception classes than should not
                              be logged.

    translation_table -- an optional table for custom value translators.

    timeout -- timeout for performing handling of async server calls.

    loop -- an optional parameter to point ZmqEventLoop instance.  If
            loop is None then default event loop will be given by
            asyncio.get_event_loop call.

    Returns Service instance.

    """
    pass


_default_error_table = _fill_error_table()


class _ClientProtocol(_BaseProtocol):
    """Client protocol implementation."""

    REQ_PREFIX = struct.Struct("=HH")
    REQ_SUFFIX = struct.Struct("=Ld")
    RESP = struct.Struct("=HHLd?")

    def __init__(self, loop, *, error_table=None, translation_table=None):
        super().__init__(loop, translation_table=translation_table)
        self.calls = {}
        self.prefix = self.REQ_PREFIX.pack(
            os.getpid() % 0x10000, random.randrange(0x10000)
        )
        self.counter = 0
        if error_table is None:
            self.error_table = _default_error_table
        else:
            self.error_table = ChainMap(error_table, _default_error_table)

    def msg_received(self, data):
        pass

    def connection_lost(self, exc):
        pass

    def _translate_error(self, exc_type, exc_args, exc_repr):
        pass

    def _new_id(self):
        pass

    def call(self, name, args, kwargs):
        pass


class RPCClient(Service):
    def __init__(self, loop, proto, *, timeout):
        super().__init__(loop, proto)
        self._timeout = timeout

    @property
    def call(self):
        """Return object for dynamic RPC calls.

        The usage is:
        ret = await client.call.ns.func(1, 2)
        """
        pass

    def with_timeout(self, timeout):
        """Return a new RPCClient instance with overriden timeout"""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        return


class _ServerProtocol(_BaseServerProtocol):

    REQ = struct.Struct("=HHLd")
    RESP_PREFIX = struct.Struct("=HH")
    RESP_SUFFIX = struct.Struct("=Ld?")

    def __init__(
        self,
        loop,
        handler,
        *,
        translation_table=None,
        log_exceptions=False,
        exclude_log_exceptions=(),
        timeout=None
    ):
        super().__init__(
            loop,
            handler,
            translation_table=translation_table,
            log_exceptions=log_exceptions,
            exclude_log_exceptions=exclude_log_exceptions,
            timeout=timeout,
        )
        self.prefix = self.RESP_PREFIX.pack(
            os.getpid() % 0x10000, random.randrange(0x10000)
        )

    def msg_received(self, data):
        pass

    def process_call_result(self, fut, *, req_id, pre, name, args, kwargs):
        pass
