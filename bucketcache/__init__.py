from __future__ import absolute_import, division, print_function

from .backends import *
from .buckets import *
from .config import *
from .exceptions import *
from .keymakers import *
from .log import logger, logger_config
from .utilities import *

__all__ = (backends.__all__ + buckets.__all__ + config.__all__ +
           exceptions.__all__ + keymakers.__all__ + utilities.__all__)

__author__ = 'Frazer McLean <frazer@frazermclean.co.uk>'
__version__ = '0.11.1'
__license__ = 'MIT'
__description__ = 'Versatile persisent file cache.'
