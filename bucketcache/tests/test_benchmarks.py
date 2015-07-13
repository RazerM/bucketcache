from __future__ import absolute_import, division

import random

import pytest

from bucketcache import deferred_write

from . import *


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
