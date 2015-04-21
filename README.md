## BucketCache
[![Build Status][bsi]][bsl] [![PyPI Version][ppi]][ppl] [![Python Version][pvi]][pvl] [![MIT License][mli]][mll]

  [bsi]: http://img.shields.io/travis/RazerM/bucketcache.svg?style=flat-square
  [bsl]: https://travis-ci.org/RazerM/bucketcache
  [ppi]: http://img.shields.io/pypi/v/bucketcache.svg?style=flat-square
  [ppl]: https://pypi.python.org/pypi/bucketcache/
  [pvi]: https://img.shields.io/badge/python-2.7%2C%203-brightgreen.svg?style=flat-square
  [pvl]: https://www.python.org/downloads/
  [mli]: http://img.shields.io/badge/license-MIT-blue.svg?style=flat-square
  [mll]: https://raw.githubusercontent.com/RazerM/bucketcache/master/LICENSE

### Installation

```bash
$ pip install bucketcache
```

### Automatic Generation

In one sentence, `Bucket` is a container object with optional lifetime backed by configurable serialisation methods that can also act as a function or method decorator.

Before everything is explained in detail, here's a quick look at the functionality:

Container:
```python
from bucketcache import Bucket

bucket = Bucket('cache', hours=1)

bucket[any_object] = anything_serializable_by_backend  # (Pickle is the default)
```

Decorator:
```python
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
```

```
Method called.
Cache used.
Method called
```

For in-depth information, [visit the documentation][doc]!

  [doc]: http://pythonhosted.org/BucketCache/