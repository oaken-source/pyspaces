'''
PySpaces implementation based on shared memory
'''

import logging
from posix_ipc import SharedMemory, ExistentialError, O_CREX

SHMEM_SIZE = 1024 * 1024 * 1024 # 1G

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
                self._shmem_name = name
                self._shmem = None

            def put(self, tpl):
                '''
                put the given tuple into the tuple space.
                '''
                raise NotImplementedError()

            def take(self, tpl):
                '''
                take the queried tuple from the tuple space and return it.
                '''
                raise NotImplementedError()

            def peek(self, tpl):
                '''
                seek the given tuple in the tuple space and return it.
                '''
                raise NotImplementedError()

            def open(self):
                '''
                connect to the shmem of the given name. this initializes the shmem, if
                it does not exist, on exactly one client
                '''
                try:
                    self._shmem = SharedMemory(self._shmem_name, size=SHMEM_SIZE, flags=O_CREX)
                    logging.info('shmem %s: created successfully', self._shmem)
                    try:
                        self._init_shmem()
                    except:
                        logging.exception('shmem %s: initialization failed', self._shmem)
                        self._shmem.unlink()
                        self._shmem.close_fd()
                        raise
                except ExistentialError:
                    self._shmem = SharedMemory(self._shmem_name)
                    logging.info('shmem %s: attached successfully', self._shmem)

            def close(self):
                '''
                close the connection to the shmem
                '''
                self._shmem.close_fd()

            def _init_shmem(self):
                '''
                perform basic initialzation of the shmem to set up the tuple space
                '''
                raise NotImplementedError()

        self._connection = PySpaceShMemConnection(*self._args, **self._kwargs)
        self._connection.open()
        return self._connection

    def __exit__(self, exc_type, exc_value, traceback):
        '''
        exit the with block
        '''
        self._connection.close()
