from __future__ import absolute_import, division

import sys

import pytest

from bucketcache import Bucket, DeferredWriteBucket
from bucketcache.backends import (
    JSONBackend, MessagePackBackend, PickleBackend)
from bucketcache.keymakers import DefaultKeyMaker, StreamingDefaultKeyMaker

cache_objects = [JSONBackend, MessagePackBackend, PickleBackend]
cache_ids = ['json', 'msgpack', 'pickle']
keymakers = [DefaultKeyMaker, StreamingDefaultKeyMaker]
keymaker_ids = ['DefaultKeyMaker', 'StreamingDefaultKeyMaker']

__all__ = [
    'slow',
    'cache_all',
    'cache_serializable',
    'expiring_cache_all',
    'deferred_cache_all',
    'requires_python_version',
    'keymakers_all',
]

slow = pytest.mark.slow

@pytest.yield_fixture(params=keymakers, ids=keymaker_ids)
def keymakers_all(request):
    yield request.param


@pytest.yield_fixture(params=cache_objects, ids=cache_ids)
def cache_all(request, tmpdir, keymakers_all):
    """Return bucket for each backend."""
    yield Bucket(path=str(tmpdir), backend=request.param)


@pytest.yield_fixture(params=[PickleBackend])
def cache_serializable(request, tmpdir):
    """Return bucket for each backend that can serialize arbitrary objects."""
    yield Bucket(path=str(tmpdir), backend=request.param)


@pytest.yield_fixture(params=cache_objects, ids=cache_ids)
def expiring_cache_all(request, tmpdir):
    """Return bucket with an expiration date for each backend."""
    yield Bucket(path=str(tmpdir), backend=request.param, seconds=2)


@pytest.yield_fixture(params=cache_objects, ids=cache_ids)
def deferred_cache_all(request, tmpdir):
    """Return deferred write bucket for each backend."""
    yield DeferredWriteBucket(path=str(tmpdir), backend=request.param)


def requires_python_version(*version):
    vbool = sys.version_info < tuple(version)
    sversion = '.'.join([str(v) for v in version])
    message = 'Requires Python {}'.format(sversion)
    return pytest.mark.skipif(vbool, reason=message)
