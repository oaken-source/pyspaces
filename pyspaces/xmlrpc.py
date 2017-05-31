'''
This module implements a simple client and server component for tuple spaces.
'''

from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer


# dirty hack <3
#list.__hash__ = tuple.__hash__


class PySpaceApi(object):
    '''
    this class implements the tuple space api on the server side.
    '''

    def __init__(self):
        '''
        constructor - prepare the tuple storage
        '''
        self._tuples = dict()

    def put(self, tpl):
        '''
        put the given tuple into the tuple space.
        '''
        tpl = tuple(tuple(v) if isinstance(v, list) else v for v in tpl)
        self._tuples[tpl] = self._tuples.get(tpl, 0) + 1

    def take(self, tpl):
        '''
        take the queried tuple from the tuple space and return it.
        '''
        tpl = tuple(tuple(v) if isinstance(v, list) else v for v in tpl)
        (t,k) = next((t,k) for (t,k) in self._tuples.items() if len(t) == len(tpl) and all(x == y or x is None for (x, y) in zip(tpl, t)))
        if k == 1:
            del self._tuples[t]
        else:
            self._tuples[t] = k - 1
        return t

    def peek(self, tpl):
        '''
        seek the given tuple in the tuple space and return it.
        '''
        t = next(t for (t,k) in self._tuples.items() if len(t) == len(tpl) and all(x == y or x is None for (x, y) in zip(tpl, t)))
        return t


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
