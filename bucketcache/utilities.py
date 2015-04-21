from __future__ import absolute_import, division, print_function

import inspect
import json
import sys
import weakref
from functools import partial, wraps

from .compat.contextlib import suppress
from .exceptions import KeyInvalidError
from .logging import logger

__all__ = ()


class CachedFunction(object):
    """Produce decorator to wrap a function with a bucket.

    The decorator function is returned because using a class breaks
    help(instance). See http://stackoverflow.com/a/25973438/2093785
    """
    def __init__(self, bucket, method=False, nocache=None, callback=None):
        self.bucket = bucket
        self.method = method
        self.nocache = nocache
        self.callback = callback
        self.fref = None

    def decorator(self, f):
        # Try and use getargspec() first so that cache will work on source
        # compatible with Python 2 and 3.
        try:
            argspec = inspect.getargspec(f)
            all_args = set(argspec.args)
        except ValueError:
            argspec = inspect.getfullargspec(f)
            all_args = set(argspec.args)
            all_args.update(argspec.kwonlyargs)

        if self.nocache:
            if self.nocache not in all_args:
                raise TypeError("nocache decorator argument '{}'"
                                "missing from argspec.".format(self.nocache))

        fsig = (f.__name__, argspec._asdict())

        self.fref = weakref.ref(f)

        def core_wrapper(key_hash, *varargs, **callargs):
            skip_cache = False
            if self.nocache:
                skip_cache = callargs[self.nocache]

            def call_and_cache():
                res = f(*varargs, **callargs)
                obj = self.bucket._update_or_make_obj_with_hash(key_hash, res)
                self.bucket._set_obj_with_hash(key_hash, obj)
                return res

            called = False
            if skip_cache:
                result = call_and_cache()
                called = True
            else:
                try:
                    obj = self.bucket._get_obj_from_hash(key_hash)
                    result = obj.value
                except KeyInvalidError:
                    result = call_and_cache()
                    called = True
                else:
                    if self.callback:
                        if self.method:
                            instance = callargs[argspec.args[0]]
                            self.callback(instance, varargs, callargs, result,
                                          obj.expiration_date)
                        else:
                            self.callback(varargs, callargs, result,
                                          obj.expiration_date)

            return result, called

        if self.method:
            @wraps(f)
            def method_wrapper(*args, **kwargs):
                instance = args[0]
                varargs, callargs = normalize_args(f, *args, **kwargs)
                sigcallargs = callargs.copy()
                # Delete instance parameter from call arg used for signature.
                del sigcallargs[argspec.args[0]]
                # Delete nocache parameter from call arg used for signature.
                if self.nocache:
                    del sigcallargs[self.nocache]

                signature = (instance.__dict__, fsig, varargs, sigcallargs)
                # Make key_hash before and after function call, and raise error
                # if any variables are modified.
                key_hash = self.bucket._hash_for_key(signature)
                ret, called = core_wrapper(key_hash, *varargs, **callargs)

                if called:
                    post_key_hash = self.bucket._hash_for_key(signature)
                    if key_hash != post_key_hash:
                        raise ValueError(
                            "modification of input parameters by "
                            "function '{}' cannot be cached.".format(f.__name__))

                return ret

            method_wrapper.callback = self.add_callback
            return method_wrapper
        else:
            @wraps(f)
            def func_wrapper(*args, **kwargs):
                varargs, callargs = normalize_args(f, *args, **kwargs)
                # Delete nocache parameter from call arg used for signature.
                if self.nocache:
                    sigcallargs = callargs.copy()
                    del sigcallargs[self.nocache]
                else:
                    sigcallargs = callargs

                signature = (fsig, varargs, sigcallargs)
                # Make key_hash before and after function call, and raise error
                # if any variables are modified.
                key_hash = self.bucket._hash_for_key(signature)
                ret, called = core_wrapper(key_hash, *varargs, **callargs)

                if called:
                    post_key_hash = self.bucket._hash_for_key(signature)
                    if key_hash != post_key_hash:
                        raise ValueError(
                            "modification of input parameters by "
                            "function '{}' cannot be cached.".format(f.__name__))

                return ret

            func_wrapper.callback = self.add_callback
            return func_wrapper

    def add_callback(self, f):
        """Magic method assigned to f.callback that allows a callback to be
        defined as follows:

        @bucket
        def fun(...):
            ...

        @fun.callback
        def fun():
            ...

        In the event that a cached result is used, the callback is fired.
        """
        self.callback = f
        return self.decorator(self.fref())


class _HashJSONEncoder(json.JSONEncoder):
    """Serialize objects that can't normally be serialized by json.

    Attempts to get state will be done in this order:

    - o.__getstate__()
    - o.__dict__
    - repr(o)

    """

    def default(self, o):
        with suppress(TypeError):
            return json.JSONEncoder.default(self, o)

        with suppress(AttributeError):
            return o.__getstate__()

        with suppress(AttributeError):
            return o.__dict__

        return repr(o)


def normalize_args(f, *args, **kwargs):
    """Normalize call arguments into keyword form and varargs.

    Returns (args, kwargs).
    args can only be non-empty if there is `*args` in the argument specification.
    """
    callargs = inspect.getcallargs(f, *args, **kwargs)
    try:
        argspec = inspect.getargspec(f)
    except ValueError:
        argspec = inspect.getfullargspec(f)

    if hasattr(argspec, 'keywords'):
        if argspec.keywords:
            kwargs = callargs.pop(argspec.keywords, {})
            callargs.update(kwargs)
    elif hasattr(argspec, 'varkw'):
        if argspec.varkw:
            kwargs = callargs.pop(argspec.varkw, {})
            callargs.update(kwargs)

    if argspec.varargs:
        varargs = callargs.pop(argspec.varargs, ())
    else:
        varargs = ()

    # now callargs is kwargs

    return varargs, callargs


def _raise_invalid_keys(valid_keys, passed_keys, message=None):
    if message is None:
        message = 'Invalid keyword argument{s}: {keys}'
    if not passed_keys <= valid_keys:
        invalid_keys = passed_keys - valid_keys
        _raise_keys(invalid_keys, message=message)


def _raise_keys(keys, message):
    invalid_str = ', '.join(keys)
    s = 's' if len(keys) > 1 else ''
    raise TypeError(message.format(s=s, keys=invalid_str))


def log_handled_exception(message, *args, **kwargs):
    logger.debug('Handled exception: ' + message, *args,
                 exc_info=sys.exc_info(), **kwargs)

# Create hash by dumping to json string with sorted keys.
_hash_dumps = partial(json.dumps, sort_keys=True, cls=_HashJSONEncoder)
