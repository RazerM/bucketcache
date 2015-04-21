from __future__ import absolute_import, division, print_function

import six
from represent import PrettyRepresentationHelper, RepresentationHelper

__all__ = (
    'PickleConfig',
    'JSONConfig',
    'MessagePackConfig',
)


class BackendConfig(object):
    def __init__(self, **kwargs):
        for name, value in six.iteritems(kwargs):
            setattr(self, name, value)

    def asdict(self):
        return {k: v for k, v in six.iteritems(self.__dict__)
                if not k.startswith('_')}

    def _repr_helper(self, r):
        for name in self.asdict():
            r.keyword_from_attr(name)

    def __repr__(self):
        r = RepresentationHelper(self)
        self._repr_helper(r)
        return str(r)

    def _repr_pretty_(self, p, cycle):
        with PrettyRepresentationHelper(self, p, cycle) as r:
            self._repr_helper(r)


class PickleConfig(BackendConfig):
    def __init__(self, protocol=2, fix_imports=True,
                 encoding='ASCII', errors='strict'):

        super(PickleConfig, self).__init__(
            protocol=protocol, fix_imports=fix_imports,
            encoding=encoding, errors=errors)


class JSONConfig(BackendConfig):
    def __init__(self, skipkeys=False, ensure_ascii=True, check_circular=True,
                 allow_nan=True, encoder_cls=None, indent=None,
                 separators=None, default=None, sort_keys=False,
                 decoder_cls=None, object_hook=None, parse_float=None,
                 parse_int=None, parse_constant=None, object_pairs_hook=None):

        super(JSONConfig, self).__init__(
            skipkeys=skipkeys,
            ensure_ascii=ensure_ascii,
            check_circular=check_circular,
            allow_nan=allow_nan,
            encoder_cls=encoder_cls,
            indent=indent,
            separators=separators,
            default=default,
            sort_keys=sort_keys,
            decoder_cls=decoder_cls,
            object_hook=object_hook,
            parse_float=parse_float,
            parse_int=parse_int,
            parse_constant=parse_constant,
            object_pairs_hook=object_pairs_hook)


class MessagePackConfig(BackendConfig):
    def __init__(self, default=None, pack_encoding='utf-8',
                 unicode_errors='strict', use_single_float=False, autoreset=1,
                 use_bin_type=1, object_hook=None, list_hook=None, use_list=1,
                 unpack_encoding='utf-8', object_pairs_hook=None):

        super(MessagePackConfig, self).__init__(
            default=default,
            pack_encoding=pack_encoding,
            unicode_errors=unicode_errors,
            use_single_float=use_single_float,
            autoreset=autoreset,
            use_bin_type=use_bin_type,
            object_hook=object_hook,
            list_hook=list_hook,
            use_list=use_list,
            unpack_encoding=unpack_encoding,
            object_pairs_hook=object_pairs_hook)
