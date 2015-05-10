from __future__ import absolute_import, division

import sys

import pytest

from bucketcache import Bucket, DeferredWriteBucket
from bucketcache.backends import (
    JSONBackend, MessagePackBackend, PickleBackend)

cache_objects = [JSONBackend, MessagePackBackend, PickleBackend]
cache_ids = ['json', 'msgpack', 'pickle']

slow = pytest.mark.slow


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


def requires_python_version(*version):
    vbool = sys.version_info < tuple(version)
    sversion = '.'.join([str(v) for v in version])
    message = 'Requires Python {}'.format(sversion)
    return pytest.mark.skipif(vbool, reason=message)
