from __future__ import absolute_import, division, print_function

import json
from abc import ABCMeta, abstractmethod
from datetime import datetime

import six
from dateutil.parser import parse
from represent import RepresentationMixin

from .compat.abc import abstractclassmethod
from .config import *
from .exceptions import *
from .utilities import _raise_invalid_keys, _raise_keys

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
class Backend(RepresentationMixin, object):
    abstract_attributes = {'binary_format', 'default_config', 'file_extension'}

    def __init__(self, value, expiration_date=None, config=None):
        self.__class__.check_concrete(skip_methods=True)

        self.value = value
        self.expiration_date = expiration_date
        self.config = self.valid_config(config)

        super(Backend, self).__init__()

    @classmethod
    def check_concrete(cls, skip_methods=False):
        """Very that we're a concrete class.

        Check that abstract methods have been overridden, and verify that
        abstract class attributes exist.

        Although ABCMeta checks the abstract methods on instantiation, we can
        also do this here to ensure the given Backend is valid when the Bucket
        is created.
        """
        if not skip_methods and cls.__abstractmethods__:
            _raise_keys(cls.__abstractmethods__, "Concrete class '{}' missing "
                        "abstract method{{s}}: {{keys}}".format(cls.__name__))

        missing = {name
                   for name in Backend.abstract_attributes
                   if not hasattr(cls, name)}
        if missing:
            _raise_keys(missing, "Concrete class '{}' missing abstract"
                        "attribute{{s}}: {{keys}}".format(cls.__name__))

    @classmethod
    def valid_config(cls, config):
        default_config = cls.default_config()
        if config is not None:
            valid_config = set(default_config.asdict())
            passed_config = set(config.asdict())
            _raise_invalid_keys(valid_config, passed_config,
                                'Invalid config parameter{s}: {keys}')
            return config
        else:
            return default_config

    def has_expired(self):
        if self.expiration_date:
            return datetime.utcnow() > self.expiration_date
        else:
            return False

    @abstractclassmethod
    def from_file(cls, fp, config=None):
        raise NotImplementedError

    @abstractmethod
    def dump(self, fp):
        raise NotImplementedError


class PickleBackend(Backend):
    binary_format = True
    default_config = PickleConfig
    file_extension = 'pickle'

    @classmethod
    def from_file(cls, fp, config=None):
        config = cls.valid_config(config)

        if six.PY3:
            dconfig = config.asdict()
            keys = ('fix_imports', 'encogding', 'errors')
            kwargs = {k: v for k, v in six.iteritems(dconfig) if k in keys}
        else:
            kwargs = dict()

        try:
            data = pickle.load(fp, **kwargs)
        except pickle.UnpicklingError as e:
            raise CacheLoadError(e)

        return cls(config=config, **data)

    def dump(self, fp):
        data = {k: self.__dict__[k] for k in ('value', 'expiration_date')}
        if six.PY3:
            dconfig = self.config.asdict()
            keys = ('protocol', 'fix_imports')
            kwargs = {k: v for k, v in six.iteritems(dconfig) if k in keys}
            pickle.dump(data, fp, **kwargs)
        else:
            pickle.dump(data, fp, protocol=self.config.protocol)


class JSONBackend(Backend):
    binary_format = False
    default_config = JSONConfig
    file_extension = 'json'

    @classmethod
    def from_file(cls, fp, config=None):
        config = cls.valid_config(config)

        dconfig = config.asdict()
        keys = ('object_hook', 'parse_float', 'parse_int',
                'parse_constant', 'object_pairs_hook')
        kwargs = {k: v for k, v in six.iteritems(dconfig) if k in keys}
        kwargs['cls'] = dconfig['decoder_cls']

        try:
            data = json.load(fp, **kwargs)
        except ValueError as e:
            raise CacheLoadError(e)
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
        kwargs = {k: v for k, v in six.iteritems(dconfig) if k in keys}
        kwargs['cls'] = dconfig['encoder_cls']

        data = {'value': self.value, 'expiration_date': expiration_date}
        json.dump(data, fp, **kwargs)


class MessagePackBackend(Backend):
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
        kwargs = {k: v for k, v in six.iteritems(dconfig) if k in keys}
        kwargs['encoding'] = dconfig['unpack_encoding']

        try:
            data = msgpack.unpack(fp, **kwargs)
        except msgpack.exceptions.ExtraData as e:
            raise CacheLoadError(e)

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
        kwargs = {k: v for k, v in six.iteritems(dconfig) if k in keys}
        kwargs['encoding'] = dconfig['pack_encoding']

        msgpack.pack(data, fp, **kwargs)
