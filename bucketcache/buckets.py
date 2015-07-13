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
from represent import ReprHelperMixin

from .backends import Backend, PickleBackend
from .compat.contextlib import suppress
from .exceptions import (
    BackendLoadError, KeyExpirationError, KeyFileNotFoundError, KeyInvalidError)
from .keymakers import DefaultKeyMaker
from .log import log_handled_exception, logger, logger_config
from .utilities import DecoratorFactory, PrunedFilesInfo, raise_invalid_keys

__all__ = ('Bucket', 'DeferredWriteBucket', 'deferred_write')


class Bucket(ReprHelperMixin, Container, object):
    """Dictionary-like object backed by a file cache.

    Parameters:
        backend: Backend class. Default:
                 :py:class:`~bucketcache.backends.PickleBackend`
        path: Directory for storing cached objects. Will be created if missing.
        config: `Config` instance for backend.
        keymaker: `KeyMaker` instance for object -> key serialization.
        lifetime: Key lifetime.
        kwargs: Keyword arguments to pass to :py:class:`datetime.timedelta`
                as shortcut for lifetime.

    :type backend: :py:class:`~bucketcache.backends.Backend`
    :type config: :py:class:`~bucketcache.config.Config`
    :type keymaker: :py:class:`~bucketcache.keymakers.KeyMaker`
    :type lifetime: :py:class:`~datetime.timedelta`
    """

    def __init__(self, path, backend=None, config=None, keymaker=None,
                 lifetime=None, **kwargs):
        if kwargs:
            valid_kwargs = {'days', 'seconds', 'microseconds', 'milliseconds',
                            'minutes', 'hours', 'weeks'}
            passed_kwargs = set(kwargs)

            raise_invalid_keys(valid_kwargs, passed_kwargs,
                               'Invalid lifetime argument{s}: {keys}')

            if lifetime:
                raise ValueError("either 'lifetime' or lifetime arguments "
                                 "('seconds', 'minutes', ...) can be passed, "
                                 "but not both.")
            lifetime = timedelta(**kwargs)

        # Now we're thinking with portals.
        self._cache = dict()

        self._path = Path(path).resolve()

        with suppress(OSError):
            self.path.mkdir()

        if backend is not None:
            self.backend = backend
        else:
            self.backend = PickleBackend

        self.config = config

        if keymaker is None:
            keymaker = DefaultKeyMaker()

        self.keymaker = keymaker

        self.lifetime = lifetime

    @property
    def path(self):
        return self._path

    @property
    def backend(self):
        return self._backend

    @backend.setter
    def backend(self, value):
        error = TypeError("'backend' must inherit from "
                          "bucketcache.Backend")
        if inspect.isclass(value):
            if not issubclass(value, Backend):
                raise error
        else:
            raise error

        value.check_concrete()
        self._backend = value

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
        """Provide setitem method as alternative to ``bucket[key] = value``"""
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
        """Provide getitem method as alternative to ``bucket[key]``."""
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
                    msg = 'File not found: {}'.format(file_path)
                    log_handled_exception(msg)
                    raise KeyFileNotFoundError(msg)
                else:
                    msg = 'Unexpected exception trying to load file: {}'
                    logger.exception(msg, file_path)
                    raise
            except BackendLoadError:
                msg = 'Backend {} failed to load file: {}'
                msg = msg.format(self.backend, file_path)
                log_handled_exception(msg)
                raise KeyInvalidError(msg)
            except Exception:
                msg = 'Unhandled exception trying to load file: {}'
                logger.exception(msg, file_path)
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

    def prune_directory(self):
        """Delete any objects that can be loaded and are expired according to
        the current lifetime setting.

        A file will be deleted if the following conditions are met:

        - The file extension matches :py:meth:`bucketcache.backends.Backend.file_extension`
        - The object can be loaded by the configured backend.
        - The object's expiration date has passed.

        Returns:
            File size and number of files deleted.

        :rtype: :py:class:`~bucketcache.utilities.PrunedFilesInfo`

        .. note::

            For any buckets that share directories, ``prune_directory`` will
            affect files saved with both, if they use the same backend class.

            This is not destructive, because only files that have expired
            according to the lifetime of the original bucket are deleted.
        """
        glob = '*.{ext}'.format(ext=self.backend.file_extension)
        totalsize = 0
        totalnum = 0
        for f in self._path.glob(glob):
            filesize = f.stat().st_size
            key_hash = f.stem
            in_cache = key_hash in self._cache
            try:
                self._get_obj_from_hash(key_hash)
            except KeyExpirationError:
                # File has been deleted by `_get_obj_from_hash`
                totalsize += filesize
                totalnum += 1
            except KeyInvalidError:
                pass
            except Exception:
                raise
            else:
                if not in_cache:
                    del self._cache[key_hash]
        return PrunedFilesInfo(size=totalsize, num=totalnum)

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

        Use `nocache='argname'` for argument that can skip cache.

        .. code:: python

            @bucket(nocache='refresh')
            def fun(refresh=False):
                ...

            fun(refresh=True)  # Cache not used.

        Use `ignore=['argname1', 'argname2', ...]` to ignore arguments when
        making cache key.

        .. code:: python

            @bucket(ignore=['log'])
            def get(name, log):
                ...

            get('spam')
            get('spam', log=True)  # Cache used even though arguments differ.
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

        raise_invalid_keys(valid_kwargs, passed_kwargs,
                           'Invalid decorator argument{s}: {keys}')

        kwargs.update({k: default_kwargs[k] for k in missing_kwargs})
        method = kwargs['method']
        nocache = kwargs['nocache']
        ignore = kwargs['ignore']

        if f:
            # We've been passed f as a standard decorator. Instantiate cached
            # function class and return the decorator.
            cf = DecoratorFactory(bucket=self, method=method, nocache=nocache,
                                  ignore=ignore)
            return cf.decorate(f)
        else:
            # We've been called with decorator arguments, so we need to return
            # a function that makes a decorator.
            cf = DecoratorFactory(bucket=self, method=method, nocache=nocache,
                                  ignore=ignore)

            def make_decorator(f):
                return cf.decorate(f)

            return make_decorator

    def _path_and_hash_for_key(self, key):
        key_hash = self._hash_for_key(key)
        path = self._path_for_hash(key_hash)
        return path, key_hash

    def _path_for_key(self, key):
        key_hash = self._hash_for_key(key)
        return self._path_for_hash(key_hash)

    def _path_for_hash(self, key_hash):
        filename = '{}.{}'.format(key_hash, self.backend.file_extension)
        return self._path / filename

    def _hash_for_key(self, key):
        if logger_config.log_full_keys:
            dkey = key
        else:
            dkey = DV(lambda: self._abbreviate(key))
        logger.debug('_hash_for_key <{}>', dkey)

        md5hash = md5(self.backend.__name__.encode('utf-8'))
        for batch in self.keymaker.make_key(key):
            md5hash.update(batch)

        logger.debug('_hash_for_key finished.')

        return md5hash.hexdigest()

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

    def _repr_helper_(self, r):
        r.keyword_with_value('path', str(self.path))
        r.keyword_from_attr('config')
        r.keyword_with_value('backend', self.backend.__name__, raw=True)
        if self.lifetime:
            for attr in ('days', 'seconds', 'microseconds'):
                value = getattr(self.lifetime, attr)
                if value:
                    r.keyword_with_value(attr, value)


class DeferredWriteBucket(Bucket):
    """Alternative implementation of :py:class:`~bucketcache.buckets.Bucket`
    that defers writing to file until
    :py:meth:`~bucketcache.buckets.DeferredWriteBucket.sync` is called.
    """
    @classmethod
    def from_bucket(cls, bucket):
        self = cls(path=bucket.path, backend=bucket.backend,
                   config=bucket.config, keymaker=bucket.keymaker,
                   lifetime=bucket.lifetime)
        self._cache = bucket._cache
        return self

    def _set_obj_with_hash(self, key_hash, obj):
        """Reimplement Bucket._set_obj_with_hash to skip writing to file."""
        self._cache[key_hash] = obj

    def unload_key(self, key):
        """Remove key from memory, leaving file in place.

        This forces :py:meth:`~bucketcache.buckets.DeferredWriteBucket.sync`.
        """
        self.sync()
        return super(DeferredWriteBucket, self).unload_key(key)

    def sync(self):
        """Commit deferred writes to file."""
        for key_hash, obj in six.iteritems(self._cache):
            # Objects are checked for expiration in __getitem__,
            # but we can check here to avoid unnecessary writes.
            if not obj.has_expired():
                file_path = self._path_for_hash(key_hash)
                with open(str(file_path), self._write_mode) as f:
                    obj.dump(f)


@contextmanager
def deferred_write(bucket):
    """Context manager for deferring writes of a :py:class:`Bucket` within a
    block.

    Parameters:
        bucket (:py:class:`Bucket`): Bucket to defer writes for within context.

    Returns:
        Bucket to use within context.

    :rtype: :py:class:`DeferredWriteBucket`

    When the context is closed, the stored objects are written to file. The
    in-memory cache of objects is used to update that of the original bucket.

    .. code-block:: python

       bucket = Bucket(path)

       with deferred_write(bucket) as deferred:
           deferred[key] = value
           ...
    """
    deferred_write_bucket = DeferredWriteBucket.from_bucket(bucket)
    yield deferred_write_bucket
    deferred_write_bucket.sync()
    bucket._cache.update(deferred_write_bucket._cache)
