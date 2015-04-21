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
        def expensive_method():
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

Decorator
---------

Buckets can be used as function **and** method decorators.

As an example, we might want to download hourly forecast data:

.. code-block:: python

    bucket = Bucket('path', hours=1)

    @bucket
    def download_weather_forecast(location):
        ...

    download_weather_forecast('Glasgow')  # Slow
    download_weather_forecast('Glasgow')  # Fast, uses cache.

We can use a callback to inform the user if the data is from the cache:

.. code-block:: python

    @download_weather_forecast.callback
    def download_weather_forecast():
        print('This data is not up-to-date. New forecasts are downloaded hourly.')

.. note::

    The original function is used as a decorator to create the callback, enforcing the order they are defined.

Let's modify the original function to allow refreshing the data manually:

.. code-block:: python

    @bucket(nocache='refresh')
    def download_weather_forecast(location, refresh=False):
        ...
    
    @download_weather_forecast.callback():
    def download_weather_forecast():
        print('This data is not up-to-date. New forecasts are downloaded hourly.')

By passing ``nocache='refresh'``, this tells the Bucket to **automatically** skip the cache if ``refresh == True``.

Using bucket as a decorator on methods is almost identical; just add ``method=True`` to the decorator arguments.

.. code-block:: python

    class WeatherForecaster():
        @bucket(method=True, nocache='refresh')
        def download(self, location, refresh=False):
            ...

        @download.callback
        def download(self):
            ...

.. note:: The attribute dictionary (``instance.__dict__``) is used to create the keys for methods.
