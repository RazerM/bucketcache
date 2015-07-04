from __future__ import absolute_import, division, print_function

import json
from abc import ABCMeta, abstractmethod
from datetime import datetime

import six
from dateutil.parser import parse
from represent import ReprMixin

from .compat.abc import abstractclassmethod
from .config import *
from .exceptions import *
from .utilities import raise_invalid_keys, raise_keys

try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    import msgpack
    msgpack_available = True
except ImportError:
    msgpack_available = False

__all__ = (
    'PickleBackend',
    'JSONBackend',
    'MessagePackBackend',
)


@six.add_metaclass(ABCMeta)
class Backend(ReprMixin, object):
    """Abstract base class for backends.

    Classes must implement abstract attributes:

    - :py:attr:`binary_format`
    - :py:attr:`default_config`
    - :py:attr:`file_extension`

    and abstract methods:

    - :py:meth:`dump`
    - :py:meth:`from_file`
    """
    abstract_attributes = {'binary_format', 'default_config', 'file_extension'}

    def __init__(self, value, expiration_date=None, config=None):
        self.__class__.check_concrete(skip_methods=True)

        self.value = value
        self.expiration_date = expiration_date
        self.config = self.valid_config(config)

        super(Backend, self).__init__()

    @classmethod
    def check_concrete(cls, skip_methods=False):
        """Verify that we're a concrete class.

        Check that abstract methods have been overridden, and verify that
        abstract class attributes exist.

        Although ABCMeta checks the abstract methods on instantiation, we can
        also do this here to ensure the given Backend is valid when the Bucket
        is created.
        """
        if not skip_methods and cls.__abstractmethods__:
            raise_keys(cls.__abstractmethods__, "Concrete class '{}' missing "
                       "abstract method{{s}}: {{keys}}".format(cls.__name__))

        missing = {name
                   for name in Backend.abstract_attributes
                   if not hasattr(cls, name)}
        if missing:
            raise_keys(missing, "Concrete class '{}' missing abstract"
                       "attribute{{s}}: {{keys}}".format(cls.__name__))

    @classmethod
    def valid_config(cls, config):
        """Verify passed config, or return `default_config`."""
        default_config = cls.default_config()
        if config is not None:
            valid_config = set(default_config.asdict())
            passed_config = set(config.asdict())
            raise_invalid_keys(valid_config, passed_config,
                               'Invalid config parameter{s}: {keys}')
            return config
        else:
            return default_config

    def has_expired(self):
        """Determine if value held by this instance has expired.

        Returns False if object does not expire.
        """
        if self.expiration_date:
            return datetime.utcnow() > self.expiration_date
        else:
            return False

    @abstractclassmethod
    def from_file(cls, fp, config=None):
        """Class method for instantiating from file.

        :param fp: File containing stored data.
        :type fp: :py:term:`file-like object`
        :param config: Configuration passed from
                       :py:class:`~bucketcache.buckets.Bucket`
        :type config: :py:class:`~bucketcache.config.Config`

        .. note::

           Implementations should ensure `config` is valid as follows:

           .. code-block:: python

              config = cls.valid_config(config)

        .. warning::

           If the appropriate data cannot be ready from `fp`, this method
           should raise :py:exc:`~bucketcache.exceptions.BackendLoadError` to
           inform :py:class:`~bucketcache.buckets.Bucket` that this cached file
           cannot be used.
        """
        raise NotImplementedError

    @abstractmethod
    def dump(self, fp):
        """Called to save state to file.

        :param fp: File to contain stored data.
        :type fp: :py:term:`file-like object`

        Attributes `value` and `expiration_date` should be saved, so that
        :py:meth:`from_file` can use them.
        """
        raise NotImplementedError


class PickleBackend(Backend):
    """Backend that serializes objects using Pickle."""
    binary_format = True
    default_config = PickleConfig
    file_extension = 'pickle'

    @classmethod
    def from_file(cls, fp, config=None):
        config = cls.valid_config(config)

        if six.PY3:
            dconfig = config.asdict()
            keys = ('fix_imports', 'encoding', 'errors')
            assert set(keys) <= set(dconfig)
            kwargs = {k: v for k, v in six.iteritems(dconfig) if k in keys}
        else:
            kwargs = dict()

        possible_exceptions = (pickle.UnpicklingError, AttributeError,
                               EOFError, ImportError, IndexError)
        try:
            data = pickle.load(fp, **kwargs)
        except possible_exceptions:
            msg = '{!r} could not be unpickled.'.format(fp.name)
            raise BackendLoadError(msg)

        return cls(config=config, **data)

    def dump(self, fp):
        data = {k: self.__dict__[k] for k in ('value', 'expiration_date')}
        if six.PY3:
            dconfig = self.config.asdict()
            keys = ('protocol', 'fix_imports')
            assert set(keys) <= set(dconfig)
            kwargs = {k: v for k, v in six.iteritems(dconfig) if k in keys}
            pickle.dump(data, fp, **kwargs)
        else:
            pickle.dump(data, fp, protocol=self.config.protocol)


class JSONBackend(Backend):
    """Backend that stores objects using JSON."""
    binary_format = False
    default_config = JSONConfig
    file_extension = 'json'

    @classmethod
    def from_file(cls, fp, config=None):
        config = cls.valid_config(config)

        dconfig = config.asdict()
        keys = ('object_hook', 'parse_float', 'parse_int',
                'parse_constant', 'object_pairs_hook')
        assert set(keys) <= set(dconfig)
        kwargs = {k: v for k, v in six.iteritems(dconfig) if k in keys}
        kwargs['cls'] = dconfig['load_cls']

        try:
            data = json.load(fp, **kwargs)
        except ValueError:
            msg = 'json file {!r} could not be loaded.'.format(fp.name)
            raise BackendLoadError(msg)
        value = data['value']
        expiration_date = data['expiration_date']
        if expiration_date:
            expiration_date = parse(expiration_date)

        return cls(config=config, value=value, expiration_date=expiration_date)

    def dump(self, fp):
        if self.expiration_date:
            expiration_date = self.expiration_date.isoformat()
        else:
            expiration_date = None

        dconfig = self.config.asdict()
        keys = ('skipkeys', 'ensure_ascii', 'check_circular', 'allow_nan',
                'indent', 'separators', 'default', 'sort_keys')
        assert set(keys) <= set(dconfig)
        kwargs = {k: v for k, v in six.iteritems(dconfig) if k in keys}
        kwargs['cls'] = dconfig['dump_cls']

        data = {'value': self.value, 'expiration_date': expiration_date}
        json.dump(data, fp, **kwargs)


class MessagePackBackend(Backend):
    """Backend that stores objects using MessagePack."""
    binary_format = True
    default_config = MessagePackConfig
    file_extension = 'msgpack'

    def __init__(self, *args, **kwargs):
        if not msgpack_available:
            raise TypeError("Please install 'msgpack-python' from PyPI.")

        super(MessagePackBackend, self).__init__(*args, **kwargs)

    @classmethod
    def from_file(cls, fp, config=None):
        config = cls.valid_config(config)

        dconfig = config.asdict()
        keys = ('object_hook', 'list_hook', 'use_list', 'object_pairs_hook')
        assert set(keys) <= set(dconfig)
        kwargs = {k: v for k, v in six.iteritems(dconfig) if k in keys}
        kwargs['encoding'] = dconfig['unpack_encoding']

        possible_exceptions = (msgpack.exceptions.ExtraData,
                               msgpack.exceptions.UnpackException)
        try:
            data = msgpack.unpack(fp, **kwargs)
        except possible_exceptions:
            msg = 'MessagePack file {!r} could not be unpacked.'.format(fp.name)
            raise BackendLoadError(msg)

        value = data['value']
        expiration_date = data['expiration_date']
        if expiration_date:
            expiration_date = parse(expiration_date)
        return cls(value=value, expiration_date=expiration_date)

    def dump(self, fp):
        if self.expiration_date:
            expiration_date = self.expiration_date.isoformat()
        else:
            expiration_date = None

        data = {'value': self.value, 'expiration_date': expiration_date}

        dconfig = self.config.asdict()
        keys = ('default', 'unicode_errors', 'use_single_float', 'autoreset',
                'use_bin_type')
        assert set(keys) <= set(dconfig)
        kwargs = {k: v for k, v in six.iteritems(dconfig) if k in keys}
        kwargs['encoding'] = dconfig['pack_encoding']

        msgpack.pack(data, fp, **kwargs)
