'''
this module is the entry point for the tuple spaces server.
'''

from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer

from pyspaces import PySpaceServer


class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    '''
    this mixin produces a threaded version of SimpleXMLRPCServer used below.
    '''
    pass


def main():
    '''
    entry point - invoked when run from the command line.
    '''
    server = ThreadedXMLRPCServer(('localhost', 10000), allow_none=True, use_builtin_types=True)
    server.register_instance(PySpaceServer())
    server.serve_forever()


if __name__ == '__main__':
    main()
