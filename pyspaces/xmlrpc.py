'''
This module implements a simple client and server component for tuple spaces.
'''

from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer


class PySpaceApi(object):
    '''
    this class implements the tuple space api on the server side.
    '''

    def __init__(self):
        '''
        constructor - prepare the tuple storage
        '''
        self._tuples = []

    def put(self, tpl):
        '''
        put the given tuple into the tuple space.
        '''
        self._tuples.append(tpl)

    def take(self, tpl):
        '''
        take the queried tuple from the tuple space and return it.
        '''
        for i, t in enumerate(self._tuples):
            if len(tpl) == len(t) and all(x == y or y is None for (x, y) in zip(tpl, t)):
                res = self._tuples[i]
                del self._tuples[i]
                return res
        return None

    def peek(self, tpl):
        '''
        seek the given tuple in the tuple space and return it.
        '''
        for i, t in enumerate(self._tuples):
            if len(tpl) == len(t) and all(x == y or y is None for (x, y) in zip(tpl, t)):
                return self._tuples[i]
        return None

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


class PySpaceXMLRPCClient(ServerProxy):
    '''
    The tuple spaces client.
    '''

    def __init__(self, server, *args, **kwargs):
        '''
        constructor - connect to the given server.
        '''
        super(PySpaceXMLRPCClient, self).__init__(
            server,
            allow_none=True,
            use_builtin_types=True,
            *args, **kwargs
        )


class PySpaceXMLRPCServer(SimpleXMLRPCServer):
    '''
    The tuple spaces server.
    '''

    def __init__(self, server, port, *args, **kwargs):
        '''
        constructor - start listening on the given server and port.
        '''
        super(PySpaceXMLRPCServer, self).__init__(
            (server, port),
            allow_none=True,
            use_builtin_types=True,
            logRequests=False,
            *args, **kwargs
        )
        self.register_instance(PySpaceApi())
