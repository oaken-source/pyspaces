
from pyspaces import PySpaceShMem

def test_simple():
    with PySpaceShMem() as space:
        space.put(('hello', 'world'))
        assert ('hello', 'world') == space.take((None, None))
