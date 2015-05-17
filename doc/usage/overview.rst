Overview
========

In one sentence, :py:class:`~bucketcache.Bucket` is a container object with optional lifetime backed by configurable serialisation methods that can also act as a function or method decorator.

Before everything is explained in detail, here's a quick look at the functionality:

.. code-block:: python

    from bucketcache import Bucket

    bucket = Bucket('cache', hours=1)

    bucket[any_object] = anything_serializable_by_backend  # (Pickle is the default)

.. code-block:: python

    class SomeService(object):
        def __init__(self, username, password):
            self.username = username
            self.password = password

        @bucket(method=True, nocache='skip_cache')
        def expensive_method(self, a, b, c, skip_cache=False):
            print('Method called.')

        @expensive_method.callback
        def expensive_method(self, callinfo):
            print('Cache used.')

    some_service = SomeService()
    some_service.expensive_method(1, 2, 3)
    some_service.expensive_method(1, 2, 3)
    some_service.expensive_method(1, 2, 3, skip_cache=True)

.. code-block:: none

    Method called.
    Cache used.
    Method called.

Bucket Creation
---------------

Path
^^^^

:py:class:`~bucketcache.Bucket` has one required parameter: `path`. This must be a :py:class:`str` or :py:class:`~pathlib.Path` directory for storing the cached files.

Lifetime
^^^^^^^^

Lifetime refers to how long a key is readable from the bucket after setting it. By default, keys do not expire.

There are two methods of setting a lifetime:

.. code-block:: python

    from datetime import timedelta

    bucket = Bucket('path', lifetime=timedelta(days=1, seconds=1, microseconds=1)))

.. code-block:: python

    bucket = Bucket('path', days=1, seconds=1, microseconds=1)

The latter is just a shortcut for the former. See :py:class:`datetime.timedelta` for all supported keyword arguments.

Backends
^^^^^^^^

Buckets can use any backend conforming to abstract class :py:class:`bucketcache.backends.Backend`. There are three provided backends:

- PickleBackend
- JSONBackend
- MessagePackBackend (if `python-msgpack`_ is installed)

.. _`python-msgpack`: https://pypi.python.org/pypi/msgpack-python/

By default, Pickle is used. Explicitly, this is specified as follows:

.. code-block:: python

    from bucketcache import PickleBackend

    bucket = Bucket('path', backend=PickleBackend)

Each backend has an associated :py:class:`bucketcache.config.BackendConfig` subclass.

For example, protocol version 4 could be used if on Python 3.4+

.. code-block:: python

    from bucketcache import PickleConfig

    bucket = Bucket('path', backend=PickleBackend, config=PickleConfig(protocol=4))

Typically, all of the parameters that can be used by the relevant `dump` or `load` methods can be specified in a config object.

Decorator
---------

Functions and methods
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    @bucket
    def function(a, b):
        ...

    class A(object):
        @bucket(method=True)
        def method(self, a, b):
            ...

    result = function(1, 2)
    result = function(1, 2)  # Cached result

    a = A()
    result = a.method(3, 4)
    result = a.method(3, 4)  # Cached result

Callback
^^^^^^^^

.. code-block:: python

    >>> @bucket
    ... def function(a, b):
    ...     return a + b

    >>> @function.callback
    ... def function(callinfo):
    ...     print(callinfo)

    >>> function(1, 2)
    3
    >>> function(1, 2)
    CachedCallInfo(varargs=(), callargs={'a': 1, 'b': 2}, return_value=3, expiration_date=datetime.datetime(2015, 1, 1, 9, 0, 0))
    3

Properties
^^^^^^^^^^

.. code-block:: python

    class A(object):
        @property
        @bucket(method=True)
        def this(self):
            ...

        @bucket(method=True)
        @property
        def that(self):
            ...

To use callback with properties, define the method first.

.. code-block:: python

    class A(object):
        @bucket(method=True)
        def this(self):
            ...

        @this.callback
        def this(self, callinfo):
            ...

        this = property(this)

Skip cache
^^^^^^^^^^

.. code-block:: python

    @bucket(nocache='refresh')
    def function(self, refresh=False):
        ...

    function()

    # Next call to function would use cached value, unless refresh==True
    function(refresh=True)

Ignored parameters
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    @bucket(ignore=['c'])
    def function(a, b, c):
        ...

    function(1, 2, 3)
    function(1, 2, 4)  # Uses cached result even though c is different

Deferred Writes
---------------

To prevent writing to file immediately, a :py:class:`~bucketcache.DeferredWriteBucket` can be used. Keys are only written to file when ``bucket.sync()`` is called.

:py:func:`bucketcache.deferred_write` is a context manager that defers writing until completion of the block.

.. code-block:: python

    from bucketcache import deferred_write

    bucket = Bucket('path')

    with deferred_write(bucket) as deferred:
        deferred[some_key] = some_value
        ...

It's also possible to create a :py:class:`~bucketcache.DeferredWriteBucket` manually:

.. code-block:: python

    from bucketcache import DeferredWriteBucket

    bucket = DeferredWriteBucket('path')

    bucket[some_key] = some_value
    ...

    bucket.sync()  # Writing happens here.

Note that calling :py:meth:`~bucketcache.DeferredWriteBucket.unload_key` on a :py:class:`~bucketcache.DeferredWriteBucket` forces a sync.
