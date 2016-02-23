from __future__ import absolute_import, division

import inspect
import sys
import textwrap

import pytest
from six import exec_

from bucketcache import DeferredWriteBucket

from . import *


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


def test_decorator_ignore(cache_all):
    cache = cache_all

    global num_called
    num_called = 0

    @cache(ignore=['message'])
    def foo(a, b, message):
        global num_called
        num_called += 1
        return a + b

    assert foo(1, 2, 'message') == 3
    assert num_called == 1
    assert foo(1, 2, 'different message') == 3
    assert num_called == 1
    assert foo(1, 1, 'different message') == 2
    assert num_called == 2

    num_called = 0

    @cache(ignore=['args'])
    def bar(a, b, *args):
        global num_called
        num_called += 1
        return a + b

    assert bar(1, 2) == 3
    assert num_called == 1
    assert bar(1, 2, 'a') == 3
    assert bar(1, 2, 'b') == 3
    assert bar(1, 2, 'b', 'c') == 3
    assert num_called == 1
    assert bar(2, 2, 'b', 'c') == 4
    assert num_called == 2

    num_called = 0

    @cache(ignore=['kwargs'])
    def spam(a, b, **kwargs):
        global num_called
        num_called += 1
        return a + b

    assert spam(1, 2) == 3
    assert num_called == 1
    assert spam(1, 2, c='a') == 3
    assert spam(1, 2, c='b') == 3
    assert spam(1, 2, c='b', d='c') == 3
    assert num_called == 1
    assert spam(2, 2, c='b', d='c') == 4
    assert num_called == 2

    num_called = 0

    @cache(ignore=['args', 'kwargs'])
    def eggs(a, b, *args, **kwargs):
        global num_called
        num_called += 1
        return a + b

    assert eggs(1, 2) == 3
    assert num_called == 1
    assert eggs(1, 2, c='a') == 3
    assert eggs(1, 2, c='b') == 3
    assert eggs(1, 2, c='b', d='c') == 3
    assert eggs(1, 2, 'b', d='c') == 3
    assert eggs(1, 2, 'c', d='c') == 3
    assert num_called == 1
    assert eggs(2, 2, c='b', d='c') == 4
    assert num_called == 2


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
    newcache = DeferredWriteBucket(path=cache._path, backend=cache.backend)

    @newcache
    def add1(a):
        add1.non_cached_calls += 1
        return a + 1

    add1.non_cached_calls = 0

    assert add1(1) == 2
    assert add1.non_cached_calls == 0
    assert add1(2) == 3


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
        assert getattr(foo, attr) == getattr(wrapped, attr), attr

    try:
        getargspec = inspect.getfullargspec
    except AttributeError:
        getargspec = inspect.getargspec

    assert getargspec(foo) == getargspec(wrapped)


if __name__ == '__main__':
    pytest.main()
