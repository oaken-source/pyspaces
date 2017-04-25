
from multiprocessing import Process
import time
import pytest
import pyspaces
from posix_ipc import ExistentialError, unlink_shared_memory, unlink_semaphore


@pytest.fixture(scope='module')
def hostname():
    return 'localhost'

@pytest.fixture(scope='module')
def port():
    return 10000

@pytest.fixture(scope='module')
def shmem_name():
    return 'PySpaceTest'

@pytest.fixture(scope='module')
def server(request, hostname, port):
    srv = pyspaces.PySpaceXMLRPCServer(hostname, port)
    p = Process(target=srv.serve_forever)
    p.start()
    yield srv
    p.terminate()
    p.join()

@pytest.fixture
def pyspace(server, hostname, port):
    return pyspaces.PySpaceXMLRPCClient('http://%s:%d' % (hostname, port))

def nothrow(error, func, args=()):
    try:
        func(*args)
    except error:
        pass

@pytest.fixture
def pyspace_shmem(shmem_name):
    nothrow(ExistentialError, unlink_shared_memory, (shmem_name,))
    nothrow(ExistentialError, unlink_semaphore, ('/pyspace_%s_lock' % shmem_name,))
    with pyspaces.PySpaceShMem(shmem_name) as space:
        yield space
    unlink_shared_memory(shmem_name)
    unlink_semaphore('/pyspace_%s_lock' % shmem_name)
