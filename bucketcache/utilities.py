from __future__ import absolute_import, division, print_function

import inspect
import json
import sys
import weakref
from collections import namedtuple
from copy import copy
from functools import partial, wraps

from decorator import decorator as decorator

from .compat.contextlib import suppress
from .exceptions import KeyInvalidError
from .log import logger

__all__ = ()


FullArgSpec = namedtuple(
    'FullArgSpec',
    ['args', 'varargs', 'varkw', 'defaults', 'kwonlyargs', 'kwonlydefaults',
     'annotations'])

NormalizedArgs = namedtuple(
    'NormalizedArgs',
    ['varargs', 'normargs', 'callargs'])

CachedCallInfo = namedtuple(
    'CachedCallInfo',
    ['varargs', 'callargs', 'return_value', 'expiration_date'])

PrunedFilesInfo = namedtuple('PrunedFilesInfo', ['size', 'num'])


def fullargspec_from_argspec(argspec):
    return FullArgSpec(
        *argspec, kwonlyargs=[], kwonlydefaults=None, annotations={})


class DecoratorFactory(object):
    """Produce decorator to wrap a function with a bucket.

    The decorator function is returned because using a class breaks
    help(instance). See http://stackoverflow.com/a/25973438/2093785
    """
    def __init__(self, bucket, method=False, nocache=None, callback=None,
                 ignore=None):
        self.bucket = bucket
        self.method = method
        self.nocache = nocache
        self.callback = callback
        self.fref = None
        self.property = False

        if ignore is None:
            ignore = ()
        self.ignore = ignore

    def decorate(self, f):

        if isinstance(f, property):
            f = f.fget
            self.property = True
            # method=True can be excluded when decorating a property because
            # it's detectable. Set it now.
            self.method = True

        self.fref = weakref.ref(f)

        # Try and use getargspec() first so that cache will work on source
        # compatible with Python 2 and 3.
        try:
            argspec = inspect.getargspec(f)
            argspec = fullargspec_from_argspec(argspec)
            all_args = set(argspec.args)
        except ValueError:
            argspec = inspect.getfullargspec(f)
            all_args = set(argspec.args)
            all_args.update(argspec.kwonlyargs)

        if self.nocache:
            if self.nocache not in all_args:
                raise TypeError("nocache decorator argument '{}'"
                                "missing from argspec.".format(self.nocache))

        test_set = set(self.ignore)

        # *args and **kwargs can be ignored too
        if argspec.varargs in self.ignore:
            test_set -= set([argspec.varargs])
        if argspec.varkw in self.ignore:
            test_set -= set([argspec.varkw])

        raise_invalid_keys(all_args, test_set,
                           message='parameter{s} cannot be ignored if not '
                                   'present in argspec: {keys}')

        fsig = (f.__name__, argspec._asdict())

        def load_or_call(f, key_hash, args, kwargs, varargs, callargs):
            """Load function result from cache, or call function and cache
            result.

            args and kwargs are used to call original function.

            varargs and callargs are used to call callback.
            """
            skip_cache = False
            if self.nocache:
                skip_cache = callargs[self.nocache]

            def call_and_cache():
                logger.info('Calling function {}', f)
                res = f(*args, **kwargs)
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
                    logger.info('Function call loaded from cache: {}', f)
                    if self.callback:
                        callinfo = CachedCallInfo(varargs, callargs, result,
                                                  obj.expiration_date)
                        if self.method:
                            instance = callargs[argspec.args[0]]
                            self.callback(instance, callinfo)
                        else:
                            self.callback(callinfo)

            return result, called

        def wrapper(f, *args, **kwargs):
            normalized_args = normalize_args(f, *args, **kwargs)
            varargs, normargs, callargs = normalized_args
            sig_normargs = normargs.copy()
            sig_varargs = copy(varargs)

            # Delete nocache parameter from call arg used for signature.
            if self.nocache:
                del sig_normargs[self.nocache]

            for arg in self.ignore:
                if arg == argspec.varargs:
                    sig_varargs = ()
                elif arg == argspec.varkw:
                    for kwarg in callargs[argspec.varkw]:
                        del sig_normargs[kwarg]
                else:
                    del sig_normargs[arg]

            if self.method:
                instance = args[0]
                # Delete instance parameter from call arg used for signature.
                del sig_normargs[argspec.args[0]]

                sig_instance = get_instance_signature(instance)

                signature = (sig_instance, fsig, sig_varargs, sig_normargs)
            else:
                signature = (fsig, sig_varargs, sig_normargs)

            # Make key_hash before function call, and raise error
            # if state changes (hash is different) afterwards.
            key_hash = self.bucket._hash_for_key(signature)
            ret, called = load_or_call(f, key_hash, args, kwargs, varargs, callargs)

            if called:
                post_key_hash = self.bucket._hash_for_key(signature)
                if key_hash != post_key_hash:
                    optional = ''
                    if self.method:
                        optional = ' or instance state'
                    raise ValueError(
                        "modification of input parameters{} by function"
                        " '{}' cannot be cached.".format(optional, f.__name__))

            return ret

        new_function = decorator(wrapper, f)
        new_function.callback = self.add_callback
        if self.property:
            new_function = property(new_function)
        return new_function

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
        return self.decorate(self.fref())


def get_instance_signature(instance):
    """Get state of instance for cache signature (as part of key).

    Attempts to get state will be done in this order:

    - instance._getsate_bucketcache_()
    - instance.__getstate__()
    - instance.__dict__
    """
    with suppress(AttributeError):
        return instance._getsate_bucketcache_()

    with suppress(AttributeError):
        return instance.__getstate__()

    return instance.__dict__


def normalize_args(f, *args, **kwargs):
    """Normalize call arguments into keyword form and varargs.

    args can only be non-empty if there is *args in the argument specification.
    """
    callargs = inspect.getcallargs(f, *args, **kwargs)
    original_callargs = callargs.copy()
    try:
        argspec = inspect.getargspec(f)
    except ValueError:
        argspec = inspect.getfullargspec(f)
    else:
        argspec = fullargspec_from_argspec(argspec)

    if hasattr(argspec, 'varkw'):
        if argspec.varkw:
            kwargs = callargs.pop(argspec.varkw, {})
            callargs.update(kwargs)

    if argspec.varargs:
        varargs = callargs.pop(argspec.varargs, ())
    else:
        varargs = ()

    # now callargs is all keywords

    return NormalizedArgs(varargs=varargs,
                          normargs=callargs,
                          callargs=original_callargs)


def raise_invalid_keys(valid_keys, passed_keys, message=None):
    if message is None:
        message = 'Invalid keyword argument{s}: {keys}'
    if not passed_keys <= valid_keys:
        invalid_keys = passed_keys - valid_keys
        raise_keys(invalid_keys, message=message)


def raise_keys(keys, message):
    invalid_str = ', '.join(keys)
    s = 's' if len(keys) > 1 else ''
    raise TypeError(message.format(s=s, keys=invalid_str))
