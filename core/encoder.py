# @Time : 2021-11-18 17:20:52
# @Author : Mio Lau
# @Contact: liurusi.101@gmail.com | github.com/MioYvo
# @File : encoder.py
# coding=utf-8
# __author__ = 'Mio'
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from json import JSONEncoder
from typing import Union, Optional
from uuid import UUID

from arrow import Arrow

unicode_type = str
_TO_UNICODE_TYPES = (unicode_type, type(None))


def to_unicode(value: Union[None, str, bytes]) -> Optional[str]:  # noqa: F811
    """Converts a string argument to a unicode string.

    If the argument is already a unicode string or None, it is returned
    unchanged.  Otherwise it must be a byte string and is decoded as utf8.
    """
    if isinstance(value, _TO_UNICODE_TYPES):
        return value
    if not isinstance(value, bytes):
        raise TypeError("Expected bytes, unicode, or None; got %r" % type(value))
    return value.decode("utf-8")


def g_str(string):
    return to_unicode(string).strip()


class MyEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, Arrow):
            return o.isoformat()
        if isinstance(o, set):
            return list(o)
        if isinstance(o, Enum):
            return o.value
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, timedelta):
            return o.total_seconds()
        # if isinstance(o, SQLAlchemyError):
        #     return str(o)
        if isinstance(o, (UUID, Decimal)):
            return str(o)

        return JSONEncoder.default(self, o)
