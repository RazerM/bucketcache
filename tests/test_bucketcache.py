from __future__ import absolute_import, division

from datetime import datetime, timedelta
from time import sleep

import pytest

from bucketcache import Bucket, deferred_write, DeferredWriteBucket
from bucketcache.backends import Backend, MessagePackBackend, PickleBackend
from bucketcache.config import PickleConfig

from . import *

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch


class CustomObject(object):
    def __init__(self, here, be, parameters):
        self.here = here
        self.be = be
        self.parameters = parameters

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


def test_retrieval(cache_all):
    """Test that setting an object allows retrieval,
    even if key is unloaded.
    """
    cache = cache_all

    cache['my key'] = 'this'
    assert cache['my key'] == 'this'
    cache.unload_key('my key')
    assert cache['my key'] == 'this'

    cache.setitem('my key', 'that')
    assert cache['my key'] == 'that'
    assert cache.getitem('my key') == 'that'


def test_unload(cache_all):
    """Test that unloading removes object from memory"""
    cache = cache_all

    cache['my key'] = 'this'
    assert cache._hash_for_key('my key') in cache._cache

    cache.unload_key('my key')
    assert cache._hash_for_key('my key') not in cache._cache


def test_missing_and_delete(cache_all):
    """Ensure KeyError raised if key doesn't exist, and that
    delete works correctly.
    """
    cache = cache_all

    with pytest.raises(KeyError):
        cache["this key doesn't exist"]

    cache['new key'] = 'banana'
    del cache['new key']
    with pytest.raises(KeyError):
        cache['new key']


def test_complex_keys(cache_all):
    """BucketCache works with any key, by serialising anything to JSON.
    Dictionary keys get sorted.
    """
    cache = cache_all

    complex_keys = [
        CustomObject('hello', [1, 2, 3], {'this': 'that'}),
        datetime.utcnow(),
    ]

    for complex_key in complex_keys:
        value = list(range(1000))
        cache[complex_key] = value
        assert cache[complex_key] == value

        cache.unload_key(complex_key)
        assert cache[complex_key] == value

    long_missing_key = list(range(1000))
    with pytest.raises(KeyError):
        cache[long_missing_key]


def test_default_keymaker(keymakers_all):
    """Test some basic assumptions of DefaultKeyMaker"""
    keymaker = keymakers_all()

    class A(object):
        """Test __getstate__ is called"""
        def __init__(self, a, b):
            self.a = a
            self.b = b

        def __getstate__(self):
            return 'getstate'

    class B(object):
        """Test classes with __slots__"""
        __slots__ = ('a', 'b', 'c')

        def __init__(self, a, b):
            self.a = a
            self.b = b

    class B2(B):
        """Test classes with empty __slots__ (to preserve parent class' slots)"""
        __slots__ = ()
        pass

    class C(object):
        """Test __dict__ is used normally."""
        def __init__(self, a, b):
            self.a = a
            self.b = b

    class D(object):
        """Test classes with __slots__ still use __getstate__"""
        __slots__ = ('a', 'b', 'c')

        def __init__(self, a, b):
            self.a = a
            self.b = b

        def __getstate__(self):
            return 'getstate'

    a = A(1, 2)
    b = B(1, 2)
    b2 = B2(1, 2)
    c = C(1, 2)
    d = D(1, 2)
    assert b''.join(keymaker.make_key(a)) == b'"getstate"'
    assert b''.join(keymaker.make_key(b)) == b'{"a": 1, "b": 2}'
    assert b''.join(keymaker.make_key(b2)) == b'{"a": 1, "b": 2}'
    assert b''.join(keymaker.make_key(c)) == b'{"a": 1, "b": 2}'
    assert b''.join(keymaker.make_key(d)) == b'"getstate"'


def test_unknown_load_error(tmpdir):
    # Ensure that unknown error in backend load from file bubbles up.

    with patch.object(PickleBackend, 'from_file', side_effect=RuntimeError):
        cache = Bucket(str(tmpdir), backend=PickleBackend)
        cache['my key'] = 'this'
        cache.unload_key('my key')
        with pytest.raises(RuntimeError):
            cache['my key']


def test_complex_values(cache_serializable):
    """Test serialization of complex values.
    Not supported by all Cache Objects.
    """
    cache = cache_serializable

    complex_values = [
        CustomObject('hello', [1, 2, 3], {'this': 'that'}),
        datetime.utcnow(),
    ]

    for i, complex_value in enumerate(complex_values):
        cache[i] = complex_value
        assert cache[i] == complex_value

        cache.unload_key(i)
        assert cache[i] == complex_value


def test_backend_missing_attributes(tmpdir):
    class MissingAttributes(Backend):
        def dump(self, fp):
            pass

        @classmethod
        def from_file(cls, fp):
            pass

    class MissingConstructor(Backend):
        binary_format = True
        default_config = PickleConfig
        file_extension = 'pickle'

        def dump(self, fp):
            pass

    with pytest.raises(TypeError):
        cache = Bucket(str(tmpdir), backend=MissingAttributes)

    with pytest.raises(TypeError):
        cache = Bucket(str(tmpdir), backend=MissingConstructor)

    with pytest.raises(TypeError):
        backend = MissingAttributes('this')

    with pytest.raises(TypeError):
        backend = MissingConstructor('that')


def test_corruption(cache_all):
    """Test loading of corrupted file."""
    cache = cache_all

    key = 'my key'
    cache[key] = 'this'
    path = cache._path_for_key(key)

    # corrupt files by writing partial data
    with open(str(path), 'r+b') as f:
        data = f.read()
        f.seek(0)
        f.write(data[1:])

    # force read from file
    cache.unload_key(key)

    with pytest.raises(KeyError):
        cache[key]


@slow
def test_expiration(expiring_cache_all):
    """Ensure keys expire."""
    cache = expiring_cache_all

    lifetime = cache.lifetime.total_seconds()

    cache['my key'] = 'this'
    sleep(lifetime)

    with pytest.raises(KeyError):
        cache['my key']

    assert 'my key' not in cache

    with pytest.raises(KeyError):
        del cache['my key']


def test_lifetime_change(cache_all):
    """Changing lifetime should force keys with larger lifetimes (or without)
    to expire.
    """
    cache = cache_all

    # Test non-expiring cache changing to finite lifetime.
    cache['my key'] = 'this'
    cache.lifetime = timedelta(seconds=2)
    lifetime = cache.lifetime.total_seconds()
    with pytest.raises(KeyError):
        cache['my key']

    # Test object expiring after new lifetime would.
    cache.lifetime = timedelta(seconds=10)
    cache['my key'] = 'this'
    cache.lifetime = timedelta(seconds=2)
    with pytest.raises(KeyError):
        cache['my key']


@slow
def test_expiration_load(expiring_cache_all):
    """Ensure that we can load cache objects with expiry information."""
    cache = expiring_cache_all

    lifetime = cache.lifetime.total_seconds()

    key = 'my key'
    cache[key] = 'this'
    sleep(lifetime / 2)
    cache.unload_key(key)
    assert cache[key] == 'this'


@slow
def test_expiration_reset(expiring_cache_all):
    """Ensure object lifetimes reset when set"""
    cache = expiring_cache_all

    lifetime = cache.lifetime.total_seconds()

    cache['my key'] = 'this'
    sleep(lifetime / 2)

    cache['my key'] = 'that'
    should_expire = datetime.utcnow() + cache.lifetime

    obj = cache._get_obj('my key')
    delta = obj.expiration_date - should_expire
    assert round(delta.total_seconds(), 1) == 0

    sleep(lifetime)
    with pytest.raises(KeyError):
        cache['my key']


def test_deferred_set(deferred_cache_all):
    """Test that DeferredWriteBucket does as it says."""
    cache = deferred_cache_all

    key = 'my key'
    cache[key] = 'this'
    path = cache._path_for_key(key)
    assert not path.exists()
    cache.sync()
    assert path.exists()

    key = 'new key'
    cache[key] = 'this'
    path = cache._path_for_key(key)
    assert not path.exists()
    cache.sync()
    assert path.exists()

    # Verify that new cache can load key from file.
    newcache = DeferredWriteBucket(path=cache._path, backend=cache.backend)
    assert newcache[key] == 'this'

    # Check updating value works
    cache[key] = 'that'
    assert cache[key] == 'that'


def test_deferred_context(cache_all):
    """Test using deffered_write context manager."""
    cache = cache_all

    key = 'my key'
    with deferred_write(cache) as deferred_cache:
        deferred_cache[key] = 'this'
        path = cache._path_for_key(key)
        assert not path.exists()
    assert path.exists()


def test_deferred_unload(deferred_cache_all):
    """Test unload_key for DeferredWriteBucket"""
    cache = deferred_cache_all

    key = 'my key'
    cache[key] = 'this'
    cache.unload_key(key)
    assert cache._hash_for_key(key) not in cache._cache
    assert cache[key] == 'this'


def test_lifetime(tmpdir):
    """Test the different ways to set cache lifetime."""
    with pytest.raises(TypeError):
        bucket = Bucket(str(tmpdir), nonsense=4)

    with pytest.raises(TypeError):
        bucket = Bucket(str(tmpdir), lifetime='wrong type')

    # test all valid lifetime args
    bucket = Bucket(str(tmpdir), days=1, seconds=1, microseconds=1,
                    milliseconds=1, minutes=1, hours=1, weeks=1)

    lifetime = timedelta(seconds=1)
    bucket = Bucket(str(tmpdir), lifetime=lifetime)

    with pytest.raises(ValueError):
        bucket = Bucket(str(tmpdir), lifetime=lifetime, seconds=4)

    with pytest.raises(TypeError):
        bucket = Bucket(str(tmpdir), lifetime=5)

    with pytest.raises(ValueError):
        bucket = Bucket(str(tmpdir), seconds=-5)


def test_backend(tmpdir):
    with pytest.raises(TypeError):
        bucket = Bucket(str(tmpdir), 5)

    with pytest.raises(TypeError):
        bucket = Bucket(str(tmpdir), CustomObject)


def test_msgpack(tmpdir):
    import bucketcache.backends

    msgpack_available = bucketcache.backends.msgpack_available
    bucketcache.backends.msgpack_available = False
    cache = Bucket(str(tmpdir), backend=MessagePackBackend)
    with pytest.raises(TypeError):
        cache['my key'] = 'this'

    # restore to previous state
    bucketcache.backends.msgpack_available = msgpack_available


if __name__ == '__main__':
    pytest.main()
