
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer

from pyspaces import PySpaceServer


class ThreadedXMLRPCServer(ThreadingMixIn,SimpleXMLRPCServer): pass

if __name__ == '__main__':
    server = ThreadedXMLRPCServer(('localhost', 10000), allow_none=True, use_builtin_types=True)
    server.register_instance(PySpaceServer())
    server.serve_forever()
