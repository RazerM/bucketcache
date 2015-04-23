from __future__ import absolute_import, division, print_function

import errno
import inspect
from collections import Container
from contextlib import contextmanager
from datetime import datetime, timedelta
from hashlib import md5
from pathlib import Path

import six
from boltons.formatutils import DeferredValue as DV
from represent import RepresentationHelper

from .backends import Backend, PickleBackend
from .compat.contextlib import suppress
from .exceptions import (
    CacheLoadError, KeyExpirationError, KeyFileNotFoundError, KeyInvalidError)
from .logging import logger, log_full_keys
from .utilities import (
    _hash_dumps, _raise_invalid_keys, CachedFunction, log_handled_exception)

__all__ = ('Bucket', 'DeferredWriteBucket', 'deferred_write')


class Bucket(Container, object):
    """Dictionary-like object backed by a file cache.

    :param path: Directory for storing cached objects.
    :param backend: Predefined or custom :py:class:`~bucketcache.Backend`
                      . Default: :py:class:`bucketcache.PickleBackend`
    :type backend: :py:class:`bucketcache.Backend`
    :param kwargs: Arguments that will be passed to
                   :py:class:`datetime.timedelta` to define
    """

    def __init__(self, path, backend=None, config=None, lifetime=None, **kwargs):
        self.path = Path(path)
        if kwargs:
            valid_kwargs = {'days', 'seconds', 'microseconds', 'milliseconds',
                            'minutes', 'hours', 'weeks'}
            passed_kwargs = set(kwargs)

            _raise_invalid_keys(valid_kwargs, passed_kwargs,
                                'Invalid lifetime argument{s}: {keys}')

            if lifetime:
                raise ValueError("either 'lifetime' or lifetime arguments "
                                 "('seconds', 'minutes', ...) can be passed, "
                                 "but not both.")
            lifetime = timedelta(**kwargs)

        # Now we're thinking with portals.
        self._cache = dict()

        with suppress(OSError):
            self.path.mkdir()
        self.path.resolve()

        if backend:
            error = TypeError("'backend' must inherit from "
                              "bucketcache.Backend")
            if inspect.isclass(backend):
                if not issubclass(backend, Backend):
                    raise error
            else:
                raise error

            backend.check_concrete()

            self.backend = backend
        else:
            self.backend = PickleBackend

        self.config = config

        self.lifetime = lifetime

    @property
    def lifetime(self):
        return self._lifetime

    @lifetime.setter
    def lifetime(self, value):
        if value is not None and value < timedelta(0):
            raise ValueError('lifetime cannot be negative.')
        if not value:
            self._lifetime = None
        else:
            if not isinstance(value, timedelta):
                raise TypeError('lifetime must be type datetime.timedelta '
                                '(or a subclass)')
            self._lifetime = value

    def __contains__(self, item):
        try:
            self[item]
        except KeyError:
            return False
        else:
            return True

    def __setitem__(self, key, value):
        key_hash = self._hash_for_key(key)
        obj = self._update_or_make_obj_with_hash(key_hash, value)
        self._set_obj_with_hash(key_hash, obj)

    def setitem(self, key, value):
        """Provide setitem method because it looks less weird when passing
        some objects as keys.

        .. seealso:: :py:meth:`__setitem__`
        """
        return self.__setitem__(key, value)

    def _update_or_make_obj_with_hash(self, key_hash, value):
        try:
            obj = self._get_obj_from_hash(key_hash, load_file=False)
            obj.value = value
        except KeyInvalidError:
            obj = self.backend(value, config=self.config)

        obj.expiration_date = self._object_expiration_date()
        return obj

    def _set_obj_with_hash(self, key_hash, obj):
        file_path = self._path_for_hash(key_hash)
        with open(str(file_path), self._write_mode) as f:
            obj.dump(f)

        self._cache[key_hash] = obj

    def __getitem__(self, key):
        obj = self._get_obj(key)

        return obj.value

    def getitem(self, key):
        """Provide setitem method because it looks less weird when passing
        some objects as keys.

        .. seealso:: :py:meth:`__getitem__`
        """
        return self.__getitem__(key)

    def _get_obj(self, key):
        key_hash = self._hash_for_key(key)
        try:
            obj = self._get_obj_from_hash(key_hash)
        except KeyInvalidError:
            raise KeyError(self._abbreviate(key))
        else:
            return obj

    def _get_obj_from_hash(self, key_hash, load_file=True):
        file_path = self._path_for_hash(key_hash)

        if key_hash in self._cache:
            obj = self._cache[key_hash]
        elif load_file:
            logger.info('Attempt load from file: {}', file_path)
            try:
                with file_path.open(self._read_mode) as f:
                    obj = self.backend.from_file(f, config=self.config)
            except IOError as e:
                if e.errno == errno.ENOENT:
                    log_handled_exception('File not found: {}', file_path)
                    raise KeyFileNotFoundError("<key hash '{}'>".format(key_hash))
                else:
                    logger.exception('Unexpected exception trying '
                                     'to load file: {}', file_path)
                    raise
            except CacheLoadError as e:
                log_handled_exception('Backend {} failed to load file: {}',
                                      self.backend, file_path)
                raise KeyInvalidError(e)
            except:
                logger.exception('Unhandled exception trying '
                                 'to load file: {}', file_path)
                raise

            self._cache[key_hash] = obj
        else:
            raise KeyInvalidError("<key hash not found in internal "
                                  "cache '{}'>".format(key_hash))

        if self.lifetime:
            # If object expires after now + lifetime, then it was saved with a
            # previous Bucket() with a longer lifetime. Let's expire the key.
            if not obj.expiration_date:
                lifetime_changed = True
            else:
                lifetime_changed = obj.expiration_date > self._object_expiration_date()
            if lifetime_changed:
                logger.warning('Object expires after now + current lifetime. '
                               'Object must have been saved with previous '
                               'cache settings. Expiring key.')
        else:
            lifetime_changed = False

        if obj.has_expired() or lifetime_changed:
            file_path.unlink()
            del self._cache[key_hash]
            raise KeyExpirationError("<key hash '{}'>".format(key_hash))

        return obj

    def __delitem__(self, key):
        file_path, key_hash = self._path_and_hash_for_key(key)
        if key in self:
            file_path.unlink()
            del self._cache[key_hash]
        else:
            raise KeyError(self._abbreviate(key))

    def unload_key(self, key):
        """Remove key from memory, leaving file in place."""
        key_hash = self._hash_for_key(key)
        if key in self:
            del self._cache[key_hash]

    def __call__(self, *args, **kwargs):
        """Use Bucket instance as a decorator.

        .. code:: python

            @bucket
            def fun(...):
                ...

        Use `method=True` for instance methods:

        .. code:: python

            @bucket(method=True)
            def fun(self, ...):
                ...
        """
        f = None
        default_kwargs = {'method': False, 'nocache': None, 'ignore': None}

        error = ('To use an instance of {}() as a decorator, '
                 'use @bucket or @bucket(<args>) '
                 '(See documentation)'.format(self.__class__.__name__))

        if len(args) + len(kwargs) < 1:
            # We need f or decorator arguments
            raise TypeError(error)

        if len(args) == 1:
            # Positional arg must be the to-be-wrapped function.
            f = args[0]

            # Allow method=True to be omitted when decorating properties, as
            # this can be detected.
            if not callable(f) and not isinstance(f, property):
                raise ValueError(error)
        elif len(args) > 1:
            raise TypeError(error)

        if len(kwargs):
            if len(args):
                raise TypeError(error)

        passed_kwargs = set(kwargs)
        valid_kwargs = set(default_kwargs)
        missing_kwargs = valid_kwargs - passed_kwargs

        _raise_invalid_keys(valid_kwargs, passed_kwargs,
                            'Invalid decorator argument{s}: {keys}')

        kwargs.update({k: default_kwargs[k] for k in missing_kwargs})
        method = kwargs['method']
        nocache = kwargs['nocache']
        ignore = kwargs['ignore']

        if f:
            # We've been passed f as a standard decorator. Instantiate cached
            # function class and return the decorator.
            cf = CachedFunction(bucket=self, method=method, nocache=nocache,
                                ignore=ignore)
            return cf.decorate(f)
        else:
            # We've been called with decorator arguments, so we need to return
            # a function that makes a decorator.
            cf = CachedFunction(bucket=self, method=method, nocache=nocache,
                                ignore=ignore)

            def make_decorator(f):
                return cf.decorate(f)

            return make_decorator

    def _path_and_hash_for_key(self, key):
        key_hash = self._hash_for_key(key)
        path = self._path_for_hash(key_hash)
        return path, key_hash

    def _path_for_key(self, key, key_hash=None):
        key_hash = self._hash_for_key(key)
        return self._path_for_hash(key_hash)

    def _path_for_hash(self, key_hash):
        filename = '{}.{}'.format(key_hash, self.backend.file_extension)
        return self.path / filename

    def _hash_for_key(self, key):
        if log_full_keys:
            dkey = key
        else:
            dkey = DV(lambda: self._abbreviate(key))
        logger.debug('_hash_for_key <{}>', dkey)

        hash_str = self.backend.__name__ + _hash_dumps(key)

        logger.debug('Done')

        digest = md5(hash_str.encode('utf-8')).hexdigest()

        return digest

    @staticmethod
    def _abbreviate(obj):
        string = repr(obj)
        if len(string) > 80:
            return string[:77] + '...'
        else:
            return string

    def _object_expiration_date(self):
        if self.lifetime:
            return datetime.utcnow() + self.lifetime
        else:
            return None

    @property
    def _read_mode(self):
        return 'rb' if self.backend.binary_format else 'r'

    @property
    def _write_mode(self):
        return 'wb' if self.backend.binary_format else 'w'

    def _repr_helper(self, r):
        r.keyword_with_value('path', str(self.path))
        r.keyword_from_attr('config')
        r.keyword_with_value('backend', self.backend.__name__, raw=True)
        if self.lifetime:
            for attr in ('days', 'seconds', 'microseconds'):
                value = getattr(self.lifetime, attr)
                if value:
                    r.keyword_with_value(attr, value)

    def __repr__(self):
        r = RepresentationHelper(self)
        self._repr_helper(r)
        return str(r)

    def _repr_pretty_(self, p, cycle):
        with PrettyRepresentationHelper(self, p, cycle) as r:
            self._repr_helper(r)


class DeferredWriteBucket(Bucket):

    @classmethod
    def from_bucket(cls, bucket):
        self = cls(path=bucket.path, backend=bucket.backend)
        self.lifetime = bucket.lifetime
        self.backend = bucket.backend
        self.config = bucket.config
        self._cache = bucket._cache
        return self

    def _set_obj_with_hash(self, key_hash, obj):
        """Reimplement Bucket._set_obj_with_hash to skip writing to file."""
        self._cache[key_hash] = obj

    def unload_key(self, key):
        """Remove key from memory, leaving file in place."""
        # Force sync to prevent data loss.
        self.sync()
        return super(DeferredWriteBucket, self).unload_key(key)

    def sync(self):
        """This is where the deferred writes get committed to file."""
        for key_hash, obj in six.iteritems(self._cache):
            # Objects are checked for expiration in __getitem__,
            # but we can check here to avoid unnecessary writes.
            if not obj.has_expired():
                file_path = self._path_for_hash(key_hash)
                with open(str(file_path), self._write_mode) as f:
                    obj.dump(f)


@contextmanager
def deferred_write(bucket):
    deferred_write_bucket = DeferredWriteBucket.from_bucket(bucket)
    yield deferred_write_bucket
    deferred_write_bucket.sync()
    bucket._cache.update(deferred_write_bucket._cache)
