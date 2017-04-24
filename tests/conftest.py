
from multiprocessing import Process
import time
import pytest
import pyspaces


@pytest.fixture(scope='module')
def hostname():
    return 'localhost'

@pytest.fixture(scope='module')
def port():
    return 10000

@pytest.fixture(scope='module')
def server(request, hostname, port):
    def init():
        srv = pyspaces.PySpaceXMLRPCServer(hostname, port)
        srv.serve_forever()
    p = Process(target=init)
    p.start()
    def fini():
        p.terminate()
        p.join()
    request.addfinalizer(fini)

@pytest.fixture
def pyspace(server, hostname, port):
    return pyspaces.PySpaceXMLRPCClient('http://%s:%d' % (hostname, port))
