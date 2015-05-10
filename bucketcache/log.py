from __future__ import absolute_import, division, print_function

import sys

from logbook import Logger

__all__ = ()

logger = Logger(__name__)
logger.disabled = True
log_full_keys = False


def log_handled_exception(message, *args, **kwargs):
    logger.debug('Handled exception: ' + message, *args,
                 exc_info=sys.exc_info(), **kwargs)
