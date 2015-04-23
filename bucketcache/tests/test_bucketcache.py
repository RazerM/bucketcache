from __future__ import absolute_import, division

import inspect
import random
import sys
import textwrap
from datetime import datetime, timedelta
from time import sleep

import pytest
from represent import RepresentationMixin
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
from six import exec_

from bucketcache import Bucket, deferred_write, DeferredWriteBucket
from bucketcache.backends import (
    Backend, JSONBackend, MessagePackBackend, PickleBackend)
from bucketcache.config import PickleConfig

cache_objects = [JSONBackend, MessagePackBackend, PickleBackend]
cache_ids = ['json', 'msgpack', 'pickle']

slow = pytest.mark.slow


def requires_python_version(*version):
    vbool = sys.version_info < tuple(version)
    sversion = '.'.join([str(v) for v in version])
    message = 'Requires Python {}'.format(sversion)
    return pytest.mark.skipif(vbool, reason=message)


class CustomObject(RepresentationMixin, object):
    def __init__(self, here, be, parameters):
        self.here = here
        self.be = be
        self.parameters = parameters

        super(CustomObject, self).__init__()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


@pytest.yield_fixture(params=cache_objects, ids=cache_ids)
def cache_all(request, tmpdir):
    yield Bucket(path=str(tmpdir), backend=request.param)


@pytest.yield_fixture(params=[PickleBackend])
def cache_serializable(request, tmpdir):
    yield Bucket(path=str(tmpdir), backend=request.param)


@pytest.yield_fixture(params=cache_objects, ids=cache_ids)
def expiring_cache_all(request, tmpdir):
    yield Bucket(path=str(tmpdir), backend=request.param, seconds=2)


@pytest.yield_fixture(params=cache_objects, ids=cache_ids)
def deferred_cache_all(request, tmpdir):
    yield DeferredWriteBucket(path=str(tmpdir), backend=request.param)


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
        range(1, 100),
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
        range(1, 100),
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


def test_property(cache_all):
    """Ensure decorator works with properties"""
    cache = cache_all

    global foo_calls, bar_calls, spam_calls, eggs_calls
    foo_calls = bar_calls = spam_calls = eggs_calls = 0
    global eggs_callback_called
    eggs_callback_called = False

    class A(object):
        # Decorate a property with explicit method=True
        @cache(method=True)
        @property
        def foo(self):
            global foo_calls
            foo_calls += 1
            return 'foo'

        # Make cached method a property
        @property
        @cache(method=True)
        def bar(self):
            global bar_calls
            bar_calls += 1
            return 'bar'

        # Decorate a property with implicit method=True
        @cache
        @property
        def spam(self):
            global spam_calls
            spam_calls += 1
            return 'spam'

        # To use callback with property, set up method first.
        @cache(method=True)
        def eggs(self):
            global eggs_calls
            eggs_calls += 1
            return 'eggs'

        @eggs.callback
        def eggs(self, callinfo):
            global eggs_callback_called
            eggs_callback_called = True

        # Now make property.
        eggs = property(eggs)

        # Alternatively, eggs could be set up with a property decorator, then
        # @eggs.fget.callback used to create the callback method.

    a = A()
    assert a.foo == 'foo'
    assert foo_calls == 1
    assert a.foo == 'foo'
    assert foo_calls == 1

    assert a.bar == 'bar'
    assert bar_calls == 1
    assert a.bar == 'bar'
    assert bar_calls == 1

    assert a.spam == 'spam'
    assert spam_calls == 1
    assert a.spam == 'spam'
    assert spam_calls == 1

    assert a.eggs == 'eggs'
    assert eggs_calls == 1
    assert not eggs_callback_called
    assert a.eggs == 'eggs'
    assert eggs_calls == 1
    assert eggs_callback_called


def test_introspection(cache_all):
    """Ensure decorator preserves function attributes and argspec."""
    cache = cache_all

    def foo(a, b, c=5, *args, **kwargs):
        """eggs"""

    wrapped = cache(foo)

    from functools import WRAPPER_ASSIGNMENTS
    for attr in WRAPPER_ASSIGNMENTS:
        if attr == '__qualname__':
            # This is not updated by decorator.decorator. Needs investigation.
            continue
        assert getattr(foo, attr) == getattr(wrapped, attr), attr

    try:
        getargspec = inspect.getfullargspec
    except AttributeError:
        getargspec = inspect.getargspec

    assert getargspec(foo) == getargspec(wrapped)


def test_decorator(cache_all):
    """Ensure decorator caching works."""
    cache = cache_all

    @cache
    def add1(a):
        add1.non_cached_calls += 1
        return a + 1

    add1.non_cached_calls = 0

    assert add1(1) == 2
    assert add1.non_cached_calls == 1
    assert add1(1) == 2
    assert add1(a=1) == 2
    assert add1.non_cached_calls == 1

    with pytest.raises(TypeError):
        @cache()
        def fun():
            pass

    with pytest.raises(TypeError):
        @cache(nonsense=4)
        def fun():
            pass

    with pytest.raises(ValueError):
        @cache(5)
        def fun():
            pass

    with pytest.raises(TypeError):
        @cache(5, 4)
        def fun():
            pass

    with pytest.raises(TypeError):
        # Test callable with keyword
        @cache(add1, nonsense=4)
        def fun():
            pass


@requires_python_version(3)
def test_decorator_fullargspec(cache_all):
    cache = cache_all

    code = """
    @cache(nocache='skip_cache')
    def fancy_py3_features(a: float, b: int, *, c: str, skip_cache=False):
        pass

    fancy_py3_features(3.5, 4, c='hey')
    """
    exec_(textwrap.dedent(code))


def test_deferred_decorator(deferred_cache_all):
    """"""
    cache = deferred_cache_all

    @cache
    def add1(a):
        add1.non_cached_calls += 1
        return a + 1

    add1.non_cached_calls = 0

    assert add1(1) == 2
    assert add1(1) == 2
    assert add1.non_cached_calls == 1

    cache.sync()

    # Verify that new cache can load key from file.
    newcache = DeferredWriteBucket(path=cache.path, backend=cache.backend)

    @newcache
    def add1(a):
        add1.non_cached_calls += 1
        return a + 1

    add1.non_cached_calls = 0

    assert add1(1) == 2
    assert add1.non_cached_calls == 0
    assert add1(2) == 3


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
    newcache = DeferredWriteBucket(path=cache.path, backend=cache.backend)
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


def test_methods(cache_all):
    """Test caching instance methods."""
    cache = cache_all

    global sum_called
    sum_called = 0

    class CachedMethods(object):
        def __init__(self, a, b, c, d):
            self.a = a
            self.b = b
            self.c = c
            self.d = d

        @cache(method=True)
        def sum(self, extra=0):
            global sum_called
            sum_called += 1
            return self.a + self.b + self.c + self.d + extra

    a = CachedMethods(2, 4, 6, 8)
    value = a.sum()
    assert sum_called == 1
    assert a.sum() == value
    assert sum_called == 1

    value = a.sum(3)
    assert sum_called == 2
    assert a.sum(3) == value
    assert sum_called == 2

    b = CachedMethods(2, 4, 6, 8)
    assert b.sum(3) == value
    assert sum_called == 2


def test_decorator_modified_arguments(cache_all):
    cache = cache_all

    @cache
    def modifies_arguments(a):
        a.append(0)
        return sum(a)

    with pytest.raises(ValueError):
        modifies_arguments([1, 2, 4])


def test_decorator_nocache(cache_all):
    """Test nocache decorator argument"""
    cache = cache_all

    global sum_called
    sum_called = 0

    # Missing argument in argspec
    with pytest.raises(TypeError):
        @cache(nocache='skip_cache')
        def some_web_api_caller(a, b):
            pass

    # Test nocache parameter without default
    @cache(nocache='skip_cache')
    def some_web_api_caller(a, b, skip_cache):
        global sum_called
        sum_called += 1
        return a + b

    assert some_web_api_caller(3, 4, False) == 7
    assert sum_called == 1
    assert some_web_api_caller(3, 4, False) == 7
    assert sum_called == 1
    assert some_web_api_caller(3, 4, True) == 7
    assert sum_called == 2
    assert some_web_api_caller(3, 4, skip_cache=False) == 7
    assert sum_called == 2
    assert some_web_api_caller(3, b=4, skip_cache=False) == 7
    assert sum_called == 2

    # Test nocache parameter with default
    @cache(nocache='skip_cache')
    def some_web_api_caller(a, b, skip_cache=False):
        global sum_called
        sum_called += 1
        return a + b

    sum_called = 0
    assert some_web_api_caller(3, 4) == 7
    assert sum_called == 1
    assert some_web_api_caller(3, 4) == 7
    assert sum_called == 1

    # Test methods using nocache
    class CachedMethods(object):
        def __init__(self, a, b, c, d):
            self.a = a
            self.b = b
            self.c = c
            self.d = d

        @cache(method=True, nocache='skip_cache')
        def sum(self, extra=0, skip_cache=False):
            global sum_called
            sum_called += 1
            return self.a + self.b + self.c + self.d + extra

    sum_called = 0

    thing = CachedMethods(2, 3, 5, 7)
    assert thing.sum() == 17
    assert sum_called == 1
    assert thing.sum() == 17
    assert sum_called == 1


def test_decorator_callback(cache_all):
    """Test decorator callbacks."""
    cache = cache_all

    @cache
    def fun(a):
        return a

    # Use global callback flag to verify whether callback gets called
    global callback_flag
    callback_flag = False

    @fun.callback
    def fun(callinfo):
        global callback_flag
        callback_flag = True

    assert fun(5) == 5
    assert not callback_flag
    assert fun(5) == 5
    assert callback_flag

    callback_flag = False

    class CachedMethods(object):
        def __init__(self, a, b, c, d):
            self.a = a
            self.b = b
            self.c = c
            self.d = d

        @cache(method=True)
        def sum(self, extra=0):
            return self.a + self.b + self.c + self.d + extra

        @sum.callback
        def sum(self, callinfo):
            global callback_flag
            callback_flag = True

    thing = CachedMethods(1, 2, 3, 4)
    assert thing.sum() == 10
    assert not callback_flag
    assert thing.sum() == 10
    assert callback_flag


def test_msgpack(tmpdir):
    import bucketcache.backends

    msgpack_available = bucketcache.backends.msgpack_available
    bucketcache.backends.msgpack_available = False
    cache = Bucket(str(tmpdir), backend=MessagePackBackend)
    with pytest.raises(TypeError):
        cache['my key'] = 'this'

    # restore to previous state
    bucketcache.backends.msgpack_available = msgpack_available


@slow
@pytest.mark.benchmark(group='call overhead')
def test_cache_overhead(cache_all, benchmark):
    """Check overhead caused by calling cached function.
    Random numbers used to avoid cache hits.
    """
    cache = cache_all

    @cache(nocache='skip_cache')
    def square(a, skip_cache=False):
        return a ** 2

    @benchmark
    def square_four():
        square(4, skip_cache=True)


@slow
@pytest.mark.benchmark(group='call overhead')
def test_cache_no_overhead(cache_all, benchmark):
    cache = cache_all

    def square(a):
        return a ** 2

    @benchmark
    def square_four():
        square(4)


@slow
@pytest.mark.benchmark(group='standard vs deferred')
def test_deferred_performance(cache_all, benchmark):
    """Test cases where keys are overwritten lots, so deferred
    write should perform quicker.

    See: test_standard_performance
    """
    cache = cache_all

    @benchmark
    def many_writes():
        with deferred_write(cache) as deferred_cache:
            for runs in range(50):
                for i in range(10):
                    deferred_cache[i] = random.random()


@slow
@pytest.mark.benchmark(group='standard vs deferred')
def test_standard_performance(cache_all, benchmark):
    cache = cache_all

    @benchmark
    def many_writes():
        for runs in range(50):
            for i in range(10):
                cache[i] = random.random()


if __name__ == '__main__':
    pytest.main()
