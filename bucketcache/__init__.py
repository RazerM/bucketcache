from __future__ import absolute_import, division, print_function

import errno
import inspect
import json
import weakref
from collections import Container
from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import wraps
from hashlib import md5
from pathlib import Path

import six
from represent import RepresentationHelper

# Import <Config>s and <Backend>s for convenient importing.
from bucketcache.backends import (
    Backend, JSONBackend, MessagePackBackend, PickleBackend)
from bucketcache.config import *
from bucketcache.exceptions import *
from bucketcache.utilities import (
    _hash_dumps, _raise_invalid_keys, normalize_args)

from .contextlib import suppress


__author__ = 'Frazer McLean <frazer@frazermclean.co.uk>'
__version__ = '1.0b1'
__license__ = 'MIT'
__description__ = 'Versatile file cache.'


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

        if not lifetime:
            self.lifetime = None
        else:
            if not isinstance(lifetime, timedelta):
                raise TypeError('lifetime must be type datetime.timedelta '
                                '(or a subclass)')
            self.lifetime = lifetime

    @property
    def lifetime(self):
        return self._lifetime

    @lifetime.setter
    def lifetime(self, value):
        if value is not None and value < timedelta(0):
            raise ValueError('lifetime cannot be negative.')
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
            obj = self._get_obj_from_hash(key_hash)
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
            raise KeyError(self._abbreviated_key(key))
        else:
            return obj

    def _get_obj_from_hash(self, key_hash):
        file_path = self._path_for_hash(key_hash)

        if key_hash in self._cache:
            obj = self._cache[key_hash]
        else:
            try:
                with file_path.open(self._read_mode) as f:
                    obj = self.backend.from_file(f, config=self.config)
            except IOError as e:
                if e.errno == errno.ENOENT:
                    raise KeyFileNotFoundError("<key hash '{}'>".format(key_hash))
                else:
                    raise
            except CacheLoadError as e:
                raise KeyInvalidError(e)
            except Exception:
                raise

            self._cache[key_hash] = obj

        if self.lifetime:
            # If object expires after now + lifetime, then it was saved with a
            # previous Bucket() with a longer lifetime. Let's expire the key.
            lifetime_changed = obj.expiration_date > self._object_expiration_date()
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
            raise KeyError(self._abbreviated_key(key))

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
        default_kwargs = {'method': False, 'nocache': None}

        error = ('To use an instance of {}() as a decorator, '
                 'use @bucket or @bucket(<args>) '
                 '(See documentation)'.format(self.__class__.__name__))

        if len(args) + len(kwargs) < 1:
            # We need f or decorator arguments
            raise TypeError(error)

        if len(args) == 1:
            # Positional arg must be the to-be-wrapped function.
            f = args[0]
            if not callable(f):
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

        if f:
            # We've been passed f as a standard decorator. Instantiate cached
            # function class and return the decorator.
            cf = CachedFunction(bucket=self, method=method, nocache=nocache)
            return cf.decorator(f)
        else:
            # We've been called with decorator arguments, so we need to return
            # a function that makes a decorator.
            cf = CachedFunction(bucket=self, method=method, nocache=nocache)

            def make_decorator(f):
                return cf.decorator(f)

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
        hash_str = self.backend.__name__ + _hash_dumps(key)
        return md5(hash_str.encode('utf-8')).hexdigest()

    def _abbreviated_key(self, key):
        key_str = "{!r}".format(key)
        if len(key_str) > 80:
            return key_str[:77] + '...'
        else:
            return key_str

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


class CachedFunction(object):
    """Produce decorator to wrap a function with a bucket.

    The decorator function is returned because using a class breaks
    help(instance). See http://stackoverflow.com/a/25973438/2093785
    """
    def __init__(self, bucket, method=False, nocache=None, callback=None):
        self.bucket = bucket
        self.method = method
        self.nocache = nocache
        self.callback = callback
        self.fref = None

    def decorator(self, f):
        # Try and use getargspec() first so that cache will work on source
        # compatible with Python 2 and 3.
        try:
            argspec = inspect.getargspec(f)
            all_args = set(argspec.args)
        except ValueError:
            argspec = inspect.getfullargspec(f)
            all_args = set(argspec.args)
            all_args.update(argspec.kwonlyargs)

        if self.nocache:
            if self.nocache not in all_args:
                raise TypeError("nocache decorator argument '{}'"
                                "missing from argspec.".format(self.nocache))

        fsig = (f.__name__, argspec._asdict())

        self.fref = weakref.ref(f)

        def core_wrapper(key_hash, *varargs, **callargs):
            skip_cache = False
            if self.nocache:
                skip_cache = callargs[self.nocache]

            def call_and_cache():
                res = f(*varargs, **callargs)
                obj = self.bucket._update_or_make_obj_with_hash(key_hash, res)
                self.bucket._set_obj_with_hash(key_hash, obj)
                return res

            if skip_cache:
                result = call_and_cache()
            else:
                try:
                    obj = self.bucket._get_obj_from_hash(key_hash)
                    result = obj.value
                except KeyInvalidError:
                    result = call_and_cache()
                else:
                    if self.callback:
                        if self.method:
                            instance = callargs[argspec.args[0]]
                            self.callback(instance)
                        else:
                            self.callback()

            return result

        if self.method:
            @wraps(f)
            def method_wrapper(*args, **kwargs):
                instance = args[0]
                varargs, callargs = normalize_args(f, *args, **kwargs)
                sigcallargs = callargs.copy()
                # Delete instance parameter from call arg used for signature.
                del sigcallargs[argspec.args[0]]
                # Delete nocache parameter from call arg used for signature.
                if self.nocache:
                    del sigcallargs[self.nocache]

                signature = (instance.__dict__, fsig, varargs, sigcallargs)
                # Make key_hash before and after function call, and raise error
                # if any variables are modified.
                key_hash = self.bucket._hash_for_key(signature)
                ret = core_wrapper(key_hash, *varargs, **callargs)

                post_key_hash = self.bucket._hash_for_key(signature)
                if key_hash != post_key_hash:
                    raise ValueError(
                        "modification of input parameters by "
                        "function '{}' cannot be cached.".format(f.__name__))

                return ret

            method_wrapper.callback = self.add_callback
            return method_wrapper
        else:
            @wraps(f)
            def func_wrapper(*args, **kwargs):
                varargs, callargs = normalize_args(f, *args, **kwargs)
                # Delete nocache parameter from call arg used for signature.
                if self.nocache:
                    sigcallargs = callargs.copy()
                    del sigcallargs[self.nocache]
                else:
                    sigcallargs = callargs

                signature = (fsig, varargs, sigcallargs)
                # Make key_hash before and after function call, and raise error
                # if any variables are modified.
                key_hash = self.bucket._hash_for_key(signature)
                ret = core_wrapper(key_hash, *varargs, **callargs)

                post_key_hash = self.bucket._hash_for_key(signature)
                if key_hash != post_key_hash:
                    raise ValueError(
                        "modification of input parameters by "
                        "function '{}' cannot be cached.".format(f.__name__))

                return ret

            func_wrapper.callback = self.add_callback
            return func_wrapper

    def add_callback(self, f):
        """Magic method assigned to f.callback that allows a callback to be
        defined as follows:

        @bucket
        def fun(...):
            ...

        @fun.callback
        def fun():
            ...

        In the event that a cached result is used, the callback is fired.
        """
        self.callback = f
        return self.decorator(self.fref())


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
