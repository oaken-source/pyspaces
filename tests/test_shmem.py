
from pyspaces import PySpaceShMem
import posix_ipc

def test_simple(pyspace_shmem):
    pyspace_shmem.put(('hello', 'world'))
    assert ('hello', 'world') == pyspace_shmem.take((None, None))
