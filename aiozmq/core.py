import asyncio
import asyncio.events
import errno
import struct
import sys
import threading
import weakref
import zmq

from collections import deque, namedtuple
from collections.abc import Iterable

from .interface import ZmqTransport, ZmqProtocol
from .log import logger
from .selector import ZmqSelector
from .util import _EndpointsSet


if sys.platform == "win32":
    from asyncio.windows_events import SelectorEventLoop
else:
    from asyncio.unix_events import SelectorEventLoop, SafeChildWatcher


__all__ = ["ZmqEventLoop", "ZmqEventLoopPolicy", "create_zmq_connection"]


SocketEvent = namedtuple("SocketEvent", "event value endpoint")


async def create_zmq_connection(
    protocol_factory, zmq_type, *, bind=None, connect=None, zmq_sock=None, loop=None
):
    """A coroutine which creates a ZeroMQ connection endpoint.

    The return value is a pair of (transport, protocol),
    where transport support ZmqTransport interface.

    protocol_factory should instantiate object with ZmqProtocol interface.

    zmq_type is type of ZeroMQ socket (zmq.REQ, zmq.REP, zmq.PUB, zmq.SUB,
    zmq.PAIR, zmq.DEALER, zmq.ROUTER, zmq.PULL, zmq.PUSH, etc.)

    bind is string or iterable of strings that specifies enpoints.
    Every endpoint creates ending for acceptin connections
    and binds it to the transport.
    Other side should use connect parameter to connect to this transport.
    See http://api.zeromq.org/master:zmq-bind for details.

    connect is string or iterable of strings that specifies enpoints.
    Every endpoint connects transport to specified transport.
    Other side should use bind parameter to wait for incoming connections.
    See http://api.zeromq.org/master:zmq-connect for details.

    endpoint is a string consisting of two parts as follows:
    transport://address.

    The transport part specifies the underlying transport protocol to use.
    The meaning of the address part is specific to the underlying
    transport protocol selected.

    The following transports are defined:

    inproc - local in-process (inter-thread) communication transport,
    see http://api.zeromq.org/master:zmq-inproc.

    ipc - local inter-process communication transport,
    see http://api.zeromq.org/master:zmq-ipc

    tcp - unicast transport using TCP,
    see http://api.zeromq.org/master:zmq_tcp

    pgm, epgm - reliable multicast transport using PGM,
    see http://api.zeromq.org/master:zmq_pgm

    zmq_sock is a zmq.Socket instance to use preexisting object
    with created transport.
    """
    pass


class ZmqEventLoop(SelectorEventLoop):
    """ZeroMQ event loop.

    Follows asyncio.AbstractEventLoop specification, in addition implements
    create_zmq_connection method for working with ZeroMQ sockets.
    """

    def __init__(self, *, zmq_context=None):
        super().__init__(selector=ZmqSelector())
        if zmq_context is None:
            self._zmq_context = zmq.Context.instance()
        else:
            self._zmq_context = zmq_context
        self._zmq_sockets = weakref.WeakSet()

    def close(self):
        pass

    async def create_zmq_connection(
        self, protocol_factory, zmq_type, *, bind=None, connect=None, zmq_sock=None
    ):
        """A coroutine which creates a ZeroMQ connection endpoint.

        See aiozmq.create_zmq_connection() coroutine for details.
        """
        pass


class _ZmqEventProtocol(ZmqProtocol):
    """This protocol is used internally by aiozmq to receive messages
    from a socket event monitor socket. This protocol decodes each event
    message into a namedtuple and then passes them through to the
    protocol running the socket that is being monitored via the
    ZmqProtocol.event_received method.

    This design simplifies the API visible to the developer at the cost
    of adding some internal complexity - a hidden protocol that transfers
    events from the monitor protocol to the monitored socket's protocol.
    """

    def __init__(self, loop, main_protocol):
        self._protocol = main_protocol
        self.wait_ready = asyncio.Future()
        self.wait_closed = asyncio.Future()

    def connection_made(self, transport):
        pass

    def connection_lost(self, exc):
        pass

    def msg_received(self, data):
        pass

    def event_received(self, evt):
        pass


class _BaseTransport(ZmqTransport):

    LOG_THRESHOLD_FOR_CONNLOST_WRITES = 5
    ZMQ_TYPES = {
        getattr(zmq, name): name
        for name in (
            "PUB",
            "SUB",
            "REP",
            "REQ",
            "PUSH",
            "PULL",
            "DEALER",
            "ROUTER",
            "XPUB",
            "XSUB",
            "PAIR",
            "STREAM",
        )
        if hasattr(zmq, name)
    }

    def __init__(self, loop, zmq_type, zmq_sock, protocol):
        super().__init__(None)
        self._protocol_paused = False
        self._set_write_buffer_limits()
        self._extra["zmq_socket"] = zmq_sock
        self._extra["zmq_type"] = zmq_type
        self._loop = loop
        self._zmq_sock = zmq_sock
        self._zmq_type = zmq_type
        self._protocol = protocol
        self._closing = False
        self._buffer = deque()
        self._buffer_size = 0
        self._bindings = set()
        self._connections = set()
        self._subscriptions = set()
        self._paused = False
        self._conn_lost = 0
        self._monitor = None

    def __repr__(self):
        info = [
            "ZmqTransport",
            "sock={}".format(self._zmq_sock),
            "type={}".format(self.ZMQ_TYPES[self._zmq_type]),
        ]
        try:
            events = self._zmq_sock.getsockopt(zmq.EVENTS)
            if events & zmq.POLLIN:
                info.append("read=polling")
            else:
                info.append("read=idle")
            if events & zmq.POLLOUT:
                state = "polling"
            else:
                state = "idle"
            bufsize = self.get_write_buffer_size()
            info.append("write=<{}, bufsize={}>".format(state, bufsize))
        except zmq.ZMQError:
            pass
        return "<{}>".format(" ".join(info))

    def write(self, data):
        pass

    def can_write_eof(self):
        pass

    def abort(self):
        pass

    def _fatal_error(self, exc, message="Fatal error on transport"):
        # Should be called from exception handler only.
        pass

    def _call_connection_lost(self, exc):
        pass

    def _maybe_pause_protocol(self):
        pass

    def _maybe_resume_protocol(self):
        pass

    def _set_write_buffer_limits(self, high=None, low=None):
        pass

    def get_write_buffer_limits(self):
        pass

    def set_write_buffer_limits(self, high=None, low=None):
        pass

    def pause_reading(self):
        pass

    def resume_reading(self):
        pass

    def getsockopt(self, option):
        pass

    def setsockopt(self, option, value):
        pass

    def get_write_buffer_size(self):
        pass

    def bind(self, endpoint):
        pass

    def unbind(self, endpoint):
        pass

    def bindings(self):
        pass

    def connect(self, endpoint):
        pass

    def disconnect(self, endpoint):
        pass

    def connections(self):
        pass

    def subscribe(self, value):
        pass

    def unsubscribe(self, value):
        pass

    def subscriptions(self):
        pass

    async def enable_monitor(self, events=None):

        # The standard approach of binding and then connecting does not
        # work in this specific case. The event loop does not properly
        # detect messages on the inproc transport which means that event
        # messages get missed.
        # pyzmq's 'get_monitor_socket' method can't be used because this
        # performs the actions in the wrong order for use with an event
        # loop.
        # For more information on this issue see:
        # http://lists.zeromq.org/pipermail/zeromq-dev/2015-July/029181.html

        pass

    async def disable_monitor(self):
        pass

    def _disable_monitor(self):
        pass


class _ZmqTransportImpl(_BaseTransport):
    def __init__(self, loop, zmq_type, zmq_sock, protocol, waiter=None):
        super().__init__(loop, zmq_type, zmq_sock, protocol)

        self._loop.add_reader(self._zmq_sock, self._read_ready)
        self._loop.call_soon(self._protocol.connection_made, self)
        if waiter is not None:
            self._loop.call_soon(waiter.set_result, None)

    def _read_ready(self):
        pass

    def _do_send(self, data):
        pass

    def _write_ready(self):
        pass

    def close(self):
        pass

    def _force_close(self, exc):
        pass

    def _do_pause_reading(self):
        pass

    def _do_resume_reading(self):
        pass


class _ZmqLooplessTransportImpl(_BaseTransport):
    def __init__(self, loop, zmq_type, zmq_sock, protocol, waiter):
        super().__init__(loop, zmq_type, zmq_sock, protocol)

        fd = zmq_sock.getsockopt(zmq.FD)
        self._fd = fd
        self._loop.add_reader(fd, self._read_ready)

        self._loop.call_soon(self._protocol.connection_made, self)
        self._loop.call_soon(waiter.set_result, None)
        self._soon_call = None

    def _read_ready(self):
        pass

    def _do_read(self):
        pass

    def _do_write(self):
        pass

    def _do_send(self, data):
        pass

    def close(self):
        pass

    def _force_close(self, exc):
        pass

    def _do_pause_reading(self):
        pass

    def _do_resume_reading(self):
        pass

    def _call_connection_lost(self, exc):
        pass


class ZmqEventLoopPolicy(asyncio.AbstractEventLoopPolicy):
    """ZeroMQ policy implementation for accessing the event loop.

    In this policy, each thread has its own event loop.  However, we
    only automatically create an event loop by default for the main
    thread; other threads by default have no event loop.
    """

    class _Local(threading.local):
        _loop = None
        _set_called = False

    def __init__(self):
        self._local = self._Local()
        self._watcher = None

    def get_event_loop(self):
        """Get the event loop.

        If current thread is the main thread and there are no
        registered event loop for current thread then the call creates
        new event loop and registers it.

        Return an instance of ZmqEventLoop.
        Raise RuntimeError if there is no registered event loop
        for current thread.
        """
        pass

    def new_event_loop(self):
        """Create a new event loop.

        You must call set_event_loop() to make this the current event
        loop.
        """
        pass

    def set_event_loop(self, loop):
        """Set the event loop.

        As a side effect, if a child watcher was set before, then calling
        .set_event_loop() from the main thread will call .attach_loop(loop) on
        the child watcher.
        """
        pass

    if sys.platform != "win32":

        def _init_watcher(self):
            pass

        def get_child_watcher(self):
            """Get the child watcher.

            If not yet set, a SafeChildWatcher object is automatically created.
            """
            pass

        def set_child_watcher(self, watcher):
            """Set the child watcher."""
            pass
