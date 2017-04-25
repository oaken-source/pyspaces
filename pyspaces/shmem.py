'''
PySpaces implementation based on shared memory
'''

import mmap
import time
import logging
import struct
import pickle
from logging.config import fileConfig
from posix_ipc import SharedMemory, Semaphore, ExistentialError, O_CREX

fileConfig('logging.ini')
LOG = logging.getLogger()

SHMEM_SIZE = 1024 * 1024 * 1024 # 1G
PYSPACE_VERSION = 1
PYSPACE_DATA_OFFSET = 0x100
PYSPACE_END = 0xffffffff

PYSPACE_FLAG_INVALID = 0x01


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
                self._mmap = None
                self._lock = None

            def put(self, tpl):
                '''
                put the given tuple into the tuple space.
                '''
                LOG.info('pyspace %s: attempting to put tuple %s', self._name, tpl)
                data = pickle.dumps(tpl)
                LOG.info('  data: %s', data)
                self._lock.acquire()
                (offset,) = struct.unpack_from('I', self._mmap, 0x10)
                LOG.info('  space claims offset of %#010x', offset)
                (length,) = struct.unpack_from('I', self._mmap, offset)
                if length != PYSPACE_END:
                    self._lock.release()
                    raise ValueError('pyspace offset data corrupt')
                LOG.info('  packing tuple metadata to offset')
                struct.pack_into('IBB', self._mmap, offset, len(data), len(tpl), 0)
                LOG.info('  writing tuple payload')
                self._mmap[offset+6:offset+6+len(data)] = data
                LOG.info('  packing PYSPACE_END')
                struct.pack_into('I', self._mmap, offset + len(data) + 6, PYSPACE_END)
                LOG.info('  updating offset data')
                struct.pack_into('I', self._mmap, 0x10, offset + len(data) + 6)
                self._lock.release()


            def take(self, tpl):
                '''
                take the queried tuple from the tuple space and return it.
                '''
                LOG.info('pyspace %s: attempting to take tuple %s', self._name, tpl)
                start = PYSPACE_DATA_OFFSET
                while True:
                    LOG.info('pyspace %s: looking at tuple at offset %#010x', self._name, start)
                    (length, fields, flags) = struct.unpack_from('IBB', self._mmap, start)
                    LOG.info('  length: %#010x, fields: %d, flags: %s', length, fields, '{0:b}'.format(flags))
                    if length == PYSPACE_END:
                        LOG.info('  tail reached. no match.')
                        return None
                    if fields != len(tpl):
                        LOG.info('  length mismatch.')
                        start += 6 + length
                        continue
                    if flags & PYSPACE_FLAG_INVALID:
                        LOG.info('  tuple invalidated.')
                        start += 6 + length
                        continue
                    data = self._mmap[start+6:start+6+length]
                    LOG.info('  fetched tuple data: %s', data)
                    data = pickle.loads(data)
                    LOG.info('  unpacked tuple data: %s', data)
                    if all(x == y or y is None for (x, y) in zip(data, tpl)):
                        LOG.info('  matching tuple found :^D')
                        self._lock.acquire()
                        # verify our view is still up to date
                        (new_flags,) = struct.unpack_from('B', self._mmap, start + 5)
                        if new_flags & PYSPACE_FLAG_INVALID:
                            LOG.info('  tuple has already been invalidated.. moving on')
                            self._lock.release()
                            start += 6 + length
                            continue
                        # this is our token now!
                        new_flags |= PYSPACE_FLAG_INVALID
                        struct.pack_into('B', self._mmap, start + 5, new_flags)
                        self._lock.release()
                        return data
                    start += 6 + length

            def peek(self, tpl):
                '''
                seek the given tuple in the tuple space and return it.
                '''
                LOG.info('pyspace %s: attempting to peek tuple %s', self._name, tpl)
                start = PYSPACE_DATA_OFFSET
                while True:
                    LOG.info('pyspace %s: looking at tuple at offset %#010x', self._name, start)
                    (length, fields, flags) = struct.unpack_from('IBB', self._mmap, start)
                    LOG.info('  length: %#010x, fields: %d, flags: %s', length, fields, '{0:b}'.format(flags))
                    if length == PYSPACE_END:
                        LOG.info('  tail reached. no match.')
                        return None
                    if fields != len(tpl):
                        LOG.info('  length mismatch.')
                        start += 6 + length
                        continue
                    if flags & PYSPACE_FLAG_INVALID:
                        LOG.info('  tuple invalidated.')
                        start += 6 + length
                        continue
                    data = pickle.loads(self._mmap[start+5:start+5+length])
                    LOG.info('  unpacked tuple data: %s', data)
                    if all(x == y or y is None for (x, y) in zip(data, tpl)):
                        LOG.info('  matching tuple found :^D')
                        return data
                    start += 6 + length

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
                shmem = SharedMemory(self._name)
                self._mmap = mmap.mmap(shmem.fd, shmem.size)
                shmem.close_fd()
                self._lock = Semaphore('/pyspace_%s_lock' % self._name)
                LOG.info('shmem %s: connect succeeded', self._name)

            def _create(self):
                '''
                attempt to create and initialize the shared memory
                '''
                LOG.info('shmem %s: attempting create shmem', self._name)
                shmem = SharedMemory(self._name, size=SHMEM_SIZE, flags=O_CREX)
                LOG.info('shmem %s: attempting create mmap', self._name)
                self._mmap = mmap.mmap(shmem.fd, shmem.size)
                shmem.close_fd()
                LOG.info('shmem %s: attempting create semaphore', self._name)
                self._lock = Semaphore('/pyspace_%s_lock' % self._name, flags=O_CREX)
                LOG.info('shmem %s: create succeeded', self._name)

                try:
                    self._initialize()
                    self._lock.release()
                except:
                    LOG.exception('shmem %s: initialize failed; attempting unlink', self._name)
                    shmem.unlink()
                    self._lock.unlink()
                    raise

            def _initialize(self):
                '''
                prepare the shmem control structures
                '''
                LOG.info('shmem %s: attempting initialize', self._name)
                LOG.info('  writing version number')
                self._mmap[8] = PYSPACE_VERSION
                LOG.info('  writing initial tail offset')
                struct.pack_into('I', self._mmap, 0x10, PYSPACE_DATA_OFFSET)
                LOG.info('  writing PYSPACE_END symbol')
                struct.pack_into('I', self._mmap, PYSPACE_DATA_OFFSET, PYSPACE_END)
                LOG.info('  writing magic number')
                self._mmap[:7] = b'pyspace'
                LOG.info('shmem %s: initialize succeeded', self._name)

        self._connection = PySpaceShMemConnection(*self._args, **self._kwargs)
        self._connection.open()
        return self._connection

    def __exit__(self, exc_type, exc_value, traceback):
        '''
        exit the with block
        '''
        self._connection.close()
