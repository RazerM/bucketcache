import json
from functools import partial

from .contextlib import suppress


class _HashJSONEncoder(json.JSONEncoder):
    """Serialize objects that can't normally be serialized by json.

    Attempts to get state will be done in this order:

    - o.__getstate__()
    - o.__dict__
    - repr(o)

    """

    def default(self, o):
        with suppress(TypeError):
            return json.JSONEncoder.default(self, o)

        with suppress(AttributeError):
            return o.__getstate__()

        with suppress(AttributeError):
            return o.__dict__

        return repr(o)

# Create hash by dumping to json string with sorted keys.
_hash_dumps = partial(json.dumps, sort_keys=True, cls=_HashJSONEncoder)


def _raise_invalid_keys(valid_keys, passed_keys, message=None):
    if message is None:
        message = 'Invalid keyword argument{s}: {keys}'
    if not passed_keys <= valid_keys:
        invalid_keys = passed_keys - valid_keys
        _raise_keys(invalid_keys, message=message)


def _raise_keys(keys, message):
    invalid_str = ', '.join(keys)
    s = 's' if len(keys) > 1 else ''
    raise TypeError(message.format(s=s, keys=invalid_str))
