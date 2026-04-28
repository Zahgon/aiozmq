"""Private utility functions."""
from collections import ChainMap
from datetime import datetime, date, time, timedelta, tzinfo
from functools import partial
from pickle import dumps, loads, HIGHEST_PROTOCOL

from msgpack import ExtType, packb, unpackb


_default = {
    127: (date, partial(dumps, protocol=HIGHEST_PROTOCOL), loads),
    126: (datetime, partial(dumps, protocol=HIGHEST_PROTOCOL), loads),
    125: (time, partial(dumps, protocol=HIGHEST_PROTOCOL), loads),
    124: (timedelta, partial(dumps, protocol=HIGHEST_PROTOCOL), loads),
    123: (tzinfo, partial(dumps, protocol=HIGHEST_PROTOCOL), loads),
}


class _Packer:
    def __init__(self, *, translation_table=None):
        if translation_table is None:
            translation_table = _default
        else:
            translation_table = ChainMap(translation_table, _default)
        self.translation_table = translation_table
        self._pack_cache = {}
        self._unpack_cache = {}
        for code in sorted(self.translation_table):
            cls, packer, unpacker = self.translation_table[code]
            self._pack_cache[cls] = (code, packer)
            self._unpack_cache[code] = unpacker

    def packb(self, data):
        pass

    def unpackb(self, packed):
        pass

    def ext_type_pack_hook(self, obj, _sentinel=object()):
        pass

    def ext_type_unpack_hook(self, code, data):
        pass
