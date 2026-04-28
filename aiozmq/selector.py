"""ZMQ pooler for asyncio."""
import math
from collections.abc import Mapping
from errno import EINTR

from zmq import (
    ZMQError,
    POLLIN,
    POLLOUT,
    POLLERR,
    Socket as ZMQSocket,
    Poller as ZMQPoller,
)


__all__ = ["ZmqSelector"]


try:
    from asyncio.selectors import BaseSelector, SelectorKey, EVENT_READ, EVENT_WRITE
except ImportError:  # pragma: no cover
    from selectors import BaseSelector, SelectorKey, EVENT_READ, EVENT_WRITE


def _fileobj_to_fd(fileobj):
    """Return a file descriptor from a file object.

    Parameters:
    fileobj -- file object or file descriptor

    Returns:
    corresponding file descriptor or zmq.Socket instance

    Raises:
    ValueError if the object is invalid
    """
    pass


class _SelectorMapping(Mapping):
    """Mapping of file objects to selector keys."""

    def __init__(self, selector):
        self._selector = selector

    def __len__(self):
        return len(self._selector._fd_to_key)

    def __getitem__(self, fileobj):
        try:
            fd = self._selector._fileobj_lookup(fileobj)
            return self._selector._fd_to_key[fd]
        except KeyError:
            raise KeyError("{!r} is not registered".format(fileobj)) from None

    def __iter__(self):
        return iter(self._selector._fd_to_key)


class ZmqSelector(BaseSelector):
    """A selector that can be used with asyncio's selector base event loops."""

    def __init__(self):
        # this maps file descriptors to keys
        self._fd_to_key = {}
        # read-only mapping returned by get_map()
        self._map = _SelectorMapping(self)
        self._poller = ZMQPoller()

    def _fileobj_lookup(self, fileobj):
        """Return a file descriptor from a file object.

        This wraps _fileobj_to_fd() to do an exhaustive search in case
        the object is invalid but we still have it in our map.  This
        is used by unregister() so we can unregister an object that
        was previously registered even if it is closed.  It is also
        used by _SelectorMapping.
        """
        pass

    def register(self, fileobj, events, data=None):
        pass

    def unregister(self, fileobj):
        pass

    def modify(self, fileobj, events, data=None):
        pass

    def close(self):
        pass

    def get_map(self):
        pass

    def _key_from_fd(self, fd):
        """Return the key associated to a given file descriptor.

        Parameters:
        fd -- file descriptor

        Returns:
        corresponding key, or None if not found
        """
        pass

    def select(self, timeout=None):
        pass
