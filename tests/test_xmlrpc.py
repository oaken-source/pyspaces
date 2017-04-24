
def test_simple(pyspace):
    pyspace.put(('hello', 'world'))
    assert ('hello', 'world') == tuple(pyspace.take((None, None)))
