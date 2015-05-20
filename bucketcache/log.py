from __future__ import absolute_import, division, print_function

import sys

from logbook import Logger
from represent import ReprMixin

__all__ = ()

logger = Logger(__name__)
logger.disabled = True


class _LoggerConfig(ReprMixin, object):
    def __init__(self, log_full_keys=False):
        self.log_full_keys = log_full_keys
        super(_LoggerConfig, self).__init__()

logger_config = _LoggerConfig()


def log_handled_exception(message, *args, **kwargs):
    logger.debug('Handled exception: ' + message, *args,
                 exc_info=sys.exc_info(), **kwargs)
