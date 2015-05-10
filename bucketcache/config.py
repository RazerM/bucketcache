from __future__ import absolute_import, division, print_function

import six
from represent import ReprMixin

__all__ = (
    'PickleConfig',
    'JSONConfig',
    'MessagePackConfig',
)


class BackendConfig(ReprMixin, object):
    """Base class for :py:class:`~bucketcache.backends.Backend` configuration
    classes.
    """
    def __init__(self):
        super(BackendConfig, self).__init__()

    def asdict(self):
        return {k: v for k, v in six.iteritems(self.__dict__)
                if not k.startswith('_')}


class PickleConfig(BackendConfig):
    """Configuration class for :py:class:`~bucketcache.backends.PickleBackend`

    Parameters reflect those given to :py:func:`pickle.dump` and
    :py:func:`pickle.load`.

    .. note::

        On Python 2, the following parameters are ignored:

        - `fix_imports`
        - `encoding`
        - `errors`
    """
    def __init__(self, protocol=2, fix_imports=True,
                 encoding='ASCII', errors='strict'):

        self.protocol = protocol
        self.fix_imports = fix_imports
        self.encoding = encoding
        self.errors = errors

        super(PickleConfig, self).__init__()


class JSONConfig(BackendConfig):
    """Configuration class for :py:class:`~bucketcache.backends.JSONBackend`

    Parameters reflect those given to :py:func:`json.dump` and
    :py:func:`json.load`.

    .. note::

        :py:func:`json.dump` and :py:func:`json.load` both have parameters
        called `cls`, so they are named `dump_cls` and `load_cls`.
    """
    def __init__(self, skipkeys=False, ensure_ascii=True, check_circular=True,
                 allow_nan=True, dump_cls=None, indent=None, separators=None,
                 default=None, sort_keys=False, load_cls=None,
                 object_hook=None, parse_float=None, parse_int=None,
                 parse_constant=None, object_pairs_hook=None):

        self.skipkeys = skipkeys
        self.ensure_ascii = ensure_ascii
        self.check_circular = check_circular
        self.allow_nan = allow_nan
        self.dump_cls = dump_cls
        self.indent = indent
        self.separators = separators
        self.default = default
        self.sort_keys = sort_keys
        self.load_cls = load_cls
        self.object_hook = object_hook
        self.parse_float = parse_float
        self.parse_int = parse_int
        self.parse_constant = parse_constant
        self.object_pairs_hook = object_pairs_hook

        super(JSONConfig, self).__init__()


class MessagePackConfig(BackendConfig):
    """Configuration class for :py:class:`~bucketcache.backends.MessagePackBackend`

    Parameters reflect those given to :py:func:`msgpack.pack` and
    :py:func:`msgpack.unpack`.

    .. note::

        :py:func:`msgpack.pack` and :py:func:`msgpack.unpack` both have
        parameters called `encoding`, so they are named `pack_encoding` and
        `unpack_encoding`.
    """
    def __init__(self, default=None, pack_encoding='utf-8',
                 unicode_errors='strict', use_single_float=False, autoreset=1,
                 use_bin_type=1, object_hook=None, list_hook=None, use_list=1,
                 unpack_encoding='utf-8', object_pairs_hook=None):

        self.default = default
        self.pack_encoding = pack_encoding
        self.unicode_errors = unicode_errors
        self.use_single_float = use_single_float
        self.autoreset = autoreset
        self.use_bin_type = use_bin_type
        self.object_hook = object_hook
        self.list_hook = list_hook
        self.use_list = use_list
        self.unpack_encoding = unpack_encoding
        self.object_pairs_hook = object_pairs_hook

        super(MessagePackConfig, self).__init__()
