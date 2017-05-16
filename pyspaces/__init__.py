'''
import pyspaces implementations to module level
'''

import logging
from logging.config import dictConfig

logging_config = dict(
    version = 1,
    formatters = {
        'f': {'format':
            '%(asctime)s [%(name)s] [%(levelname)s] :: %(message)s'}
        },
    handlers = {
        'h': {'class': 'logging.StreamHandler',
            'formatter': 'f',
            'level': logging.WARNING}
        },
    root = {
        'handlers': ['h'],
        'level': logging.WARNING,
        },
    )

dictConfig(logging_config)

from .xmlrpc import PySpaceXMLRPCClient, PySpaceXMLRPCServer
from .shmem import PySpaceShMem
