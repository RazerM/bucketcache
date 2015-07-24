from __future__ import absolute_import, division, print_function

import json
from abc import ABCMeta, abstractmethod
from functools import partial
from tempfile import TemporaryFile

import six
from represent import ReprMixin

from .compat.contextlib import suppress

__all__ = (
    'DefaultKeyMaker',
    'StreamingDefaultKeyMaker',
)


@six.add_metaclass(ABCMeta)
class KeyMaker(ReprMixin, object):
    """KeyMaker abstract base class."""
    def __init__(self):
        super(KeyMaker, self).__init__()

    @abstractmethod
    def make_key(self, obj):
        """Make key from passed object.

        Parameters:
            obj: Any Python object.

        Yields:
            bytes of key to represent object.
        """
        raise NotImplementedError


class DefaultKeyMaker(KeyMaker):
    """Default KeyMaker that is consistent across Python versions.

    Uses :py:class:`_AnyObjectJSONEncoder` to convert any object into a string
    representation.

    Parameters:
        sort_keys (bool): Sort dictionary keys for consistency across Python
                          versions with different hash algorithms.
    """
    def __init__(self, sort_keys=True):
        self.sort_keys = sort_keys
        super(DefaultKeyMaker, self).__init__()

    def make_key(self, obj):
        keystr = json.dumps(
            obj, sort_keys=self.sort_keys, cls=_AnyObjectJSONEncoder)
        yield keystr.encode('utf-8')


class StreamingDefaultKeyMaker(DefaultKeyMaker):
    """Subclass of DefaultKeyMaker that uses a temporary file to save memory."""
    def make_key(self, obj):
        with TemporaryFile(mode='w+') as f:
            json.dump(
                obj, f, sort_keys=self.sort_keys, cls=_AnyObjectJSONEncoder)
            f.seek(0)
            for data in iter(partial(f.read, 65536), ''):
                yield data.encode('utf-8')


class _AnyObjectJSONEncoder(json.JSONEncoder):
    """Serialize objects that can't normally be serialized by json.

    Attempts to get state will be done in this order:

    - ``o.__getstate__()``
    - Parameters from ``o.__slots__``
    - ``o.__dict__``
    - ``repr(o)``
    """

    def default(self, o):
        with suppress(TypeError):
            return json.JSONEncoder.default(self, o)

        with suppress(AttributeError):
            return o.__getstate__()

        if hasattr(o, '__slots__'):
            all_slots = set()
            for cls in o.__class__.__mro__:
                slots = getattr(cls, '__slots__', tuple())
                slots = normalise_slots(slots)
                all_slots.update(slots)

            return {k: getattr(o, k) for k in all_slots if hasattr(o, k)}

        with suppress(AttributeError):
            return o.__dict__

        return repr(o)

def normalise_slots(obj):
    """__slots__ can be a string for single attribute. Return inside tuple."""
    if isinstance(obj, six.string_types):
        return (obj,)
    else:
        return obj
