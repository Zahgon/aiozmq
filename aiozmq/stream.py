import collections
import asyncio
from .core import create_zmq_connection
from .interface import ZmqProtocol


class ZmqStreamClosed(Exception):
    """A stream was closed"""


async def create_zmq_stream(
    zmq_type,
    *,
    bind=None,
    connect=None,
    loop=None,
    zmq_sock=None,
    high_read=None,
    low_read=None,
    high_write=None,
    low_write=None,
    events_backlog=100
):
    """A wrapper for create_zmq_connection() returning a Stream instance.

    The arguments are all the usual arguments to create_zmq_connection()
    except protocol_factory; most common are positional host and port,
    with various optional keyword arguments following.

    Additional optional keyword arguments are loop (to set the event
    loop instance to use) and high_read, low_read, high_write,
    low_write -- high and low watermarks for reading and writing
    respectively.

    events_backlog -- backlog size for monitoring events, 100 by
    default.  It specifies size of event queue. If count of unread
    events exceeds events_backlog the oldest events are discarded.

    """
    pass


class ZmqStreamProtocol(ZmqProtocol):
    """Helper class to adapt between ZmqProtocol and ZmqStream.

    This is a helper class to use ZmqStream instead of subclassing
    ZmqProtocol.
    """

    def __init__(self, stream, loop):
        self._loop = loop
        self._stream = stream
        self._paused = False
        self._drain_waiter = None
        self._connection_lost = False

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def connection_made(self, transport):
        pass

    def connection_lost(self, exc):
        pass

    async def _drain_helper(self):
        pass

    def msg_received(self, msg):
        pass

    def event_received(self, event):
        pass


class ZmqStream:
    """Wraps a ZmqTransport.

    Has write() method and read() coroutine for writing and reading
    ZMQ messages.

    It adds drain() coroutine which can be used for waiting for flow
    control.

    It also adds a transport property which references the
    ZmqTransport directly.

    """

    def __init__(self, loop, *, high=None, low=None, events_backlog=100):
        self._transport = None
        self._protocol = ZmqStreamProtocol(self, loop=loop)
        self._loop = loop
        self._queue = collections.deque()
        self._event_queue = collections.deque(maxlen=events_backlog)
        self._closing = False  # Whether we're done.
        self._waiter = None  # A future.
        self._event_waiter = None  # A future.
        self._exception = None
        self._paused = False
        self._set_read_buffer_limits(high, low)
        self._queue_len = 0

    @property
    def transport(self):
        pass

    def write(self, msg):
        pass

    def close(self):
        pass

    def get_extra_info(self, name, default=None):
        pass

    async def drain(self):
        """Flush the write buffer.

        The intended use is to write

          w.write(data)
          await w.drain()
        """
        pass

    def exception(self):
        pass

    def set_exception(self, exc):
        """Private"""
        pass

    def set_transport(self, transport):
        """Private"""
        pass

    def _set_read_buffer_limits(self, high=None, low=None):
        pass

    def set_read_buffer_limits(self, high=None, low=None):
        pass

    def _maybe_resume_transport(self):
        pass

    def feed_closing(self):
        """Private"""
        pass

    def at_closing(self):
        """Return True if the buffer is empty and 'feed_closing' was called."""
        pass

    def feed_msg(self, msg):
        """Private"""
        pass

    def feed_event(self, event):
        """Private"""
        pass

    async def read(self):
        pass

    async def read_event(self):
        pass
