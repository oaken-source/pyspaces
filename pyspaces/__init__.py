'''
This module implements a simple client and server component for tuple spaces.
'''

from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer
from socketserver import ThreadingMixIn
from threading import Lock, Condition


class PySpaceApi(object):
    '''
    this class implements the tuple space api on the server side.
    '''

    def __init__(self):
        '''
        constructor - prepare the tuple storage and the synchronization
        primitives.
        '''
        self._tuples = []
        self._condition = Condition()

    def put(self, tpl):
        '''
        put the given tuple into the tuple space.
        '''
        with self._condition:
            self._tuples.append(tpl)
            self._condition.notify_all()

    def take(self, tpl):
        '''
        take the queried tuple from the tuple space and return it.
        '''
        while True:
            with self._condition:
                res = self._find(tpl)
                if res is not None:
                    self._tuples.remove(res)
                    return res
                self._condition.wait()

    def peek(self, tpl):
        '''
        seek the given tuple in the tuple space and return it.
        '''
        while True:
            with self._condition:
                res = self._find(tpl)
                if res is not None:
                    return res
                self._condition.wait()

    def _find(self, tpl):
        '''
        find the queried tuple in the tuple space and return it, or None if
        not found.
        '''
        return next((
            res
            for res in self._tuples
            if len(res) == len(tpl) and all(x == y or y is None for (x, y) in zip(res, tpl))
        ), None)


class PySpace(ServerProxy):
    '''
    The tuple spaces client.
    '''

    def __init__(self, server):
        '''
        constructor - connect to the given server.
        '''
        super(PySpace, self).__init__(server, allow_none=True, use_builtin_types=True)


class PySpaceServer(ThreadingMixIn, SimpleXMLRPCServer):
    '''
    The tuple spaces server.
    '''

    def __init__(self, server, port):
        '''
        constructor - start listening on the given server and port.
        '''
        super(PySpaceServer, self).__init__(
            (server, port),
            allow_none=True,
            use_builtin_types=True
        )
        self.register_instance(PySpaceApi())
