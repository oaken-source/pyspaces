'''
PySpaces implementation based on shared memory
'''

import mmap
import time
import logging
from logging.config import fileConfig
from posix_ipc import SharedMemory, Semaphore, ExistentialError, O_CREX

fileConfig('logging.ini')
LOG = logging.getLogger()

SHMEM_SIZE = 1024 * 1024 * 1024 # 1G
PYSPACE_VERSION = 1


class PySpaceShMem(object):
    '''
    this class implements the pyspace connection resource manager
    '''
    def __init__(self, *args, **kwargs):
        '''
        constructor - pass args to connection class
        '''
        self._args = args
        self._kwargs = kwargs
        self._connection = None

    def __enter__(self):
        '''
        enter the with block
        '''
        class PySpaceShMemConnection(object):
            '''
            this class implements a pyspace shmem participart
            '''
            def __init__(self, name='PySpace'):
                '''
                constructor - connect to the shmem
                '''
                self._name = name
                self._shmem = None
                self._mmap = None
                self._lock = None

            def put(self, tpl):
                '''
                put the given tuple into the tuple space.
                '''
                pass

            def take(self, tpl):
                '''
                take the queried tuple from the tuple space and return it.
                '''
                return ('hello', 'world')

            def peek(self, tpl):
                '''
                seek the given tuple in the tuple space and return it.
                '''
                return ('hello', 'world')

            def open(self):
                '''
                connect to the shmem of the given name. this initializes the shmem, if
                it does not exist, on exactly one client
                '''
                try:
                    self._connect()
                except ExistentialError:
                    LOG.warning('shmem %s: connect failed, need to create', self._name)
                    try:
                        self._create()
                    except ExistentialError:
                        LOG.warning('shmem %s: create failed, someone was faster', self._name)
                        self._connect()

                # wait for space to be initialized
                tries = 0
                while self._mmap[:7] != b'pyspace':
                    time.sleep(1)
                    tries += 1
                    if tries >= 10:
                        raise ValueError('PySpace wait timed out - corrupted?')
                if self._mmap[8] != PYSPACE_VERSION:
                    raise ValueError('PySpace version mismatch')

            def close(self):
                '''
                close the connection to the shmem
                '''
                self._mmap.close()
                self._lock.close()

            def _connect(self):
                '''
                attempt to connect to the shared memory
                '''
                LOG.info('shmem %s: attempting connect', self._name)
                self._shmem = SharedMemory(self._name)
                self._mmap = mmap.mmap(self._shmem.fd, self._shmem.size)
                self._shmem.close_fd()
                self._lock = Semaphore('/pyspace_%s_lock' % self._name)
                LOG.info('shmem %s: connect succeeded', self._name)

            def _create(self):
                '''
                attempt to create and initialize the shared memory
                '''
                LOG.info('shmem %s: attempting create', self._name)
                self._shmem = SharedMemory(self._name, size=SHMEM_SIZE, flags=O_CREX)
                self._mmap = mmap.mmap(self._shmem.fd, self._shmem.size)
                self._shmem.close_fd()
                self._lock = Semaphore('/pyspace_%s_lock' % self._name, flags=O_CREX)
                LOG.info('shmem %s: create succeeded', self._name)

                try:
                    self._initialize()
                except:
                    LOG.exception('shmem %s: initialize failed; attempting unlink', self._name)
                    self._shmem.unlink()
                    self._lock.unlink()
                    raise

            def _initialize(self):
                '''
                prepare the shmem control structures
                '''
                LOG.info('shmem %s: attempting initialize', self._name)
                self._mmap[8] = PYSPACE_VERSION

                # write magic number last, other clients wait for it
                self._mmap[:7] = b'pyspace'
                LOG.info('shmem %s: initialize successful', self._name)

        self._connection = PySpaceShMemConnection(*self._args, **self._kwargs)
        self._connection.open()
        return self._connection

    def __exit__(self, exc_type, exc_value, traceback):
        '''
        exit the with block
        '''
        self._connection.close()
