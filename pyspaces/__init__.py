
from xmlrpc.client import ServerProxy
from threading import Lock, Condition


class PySpace(object):

    def __init__(self, server):
        self._server = server
        self._proxy = ServerProxy(server, allow_none=True, use_builtin_types=True)

    def put(self, t):
        if not isinstance(t, tuple):
            raise ValueError('parameter needs to be a tuple')
        return self._proxy.put(t)

    def take(self, t):
        if not isinstance(t, tuple):
            raise ValueError('parameter needs to be a tuple')
        return tuple(self._proxy.take(t))

    def peek(self, t):
        if not isinstance(t, tuple):
            raise ValueError('parameter needs to be a tuple')
        return tuple(self._proxy.peek(t))


class PySpaceServer(object):

    def __init__(self):
        self._tuples = []

        self._lock = Lock()
        self._condition = Condition()

    def put(self, t):
        with self._condition:
            self._tuples.append(tuple(t))
            self._condition.notify_all()

    def take(self, t):
        while True:
            with self._condition:
                r = self._find(t)
                if r is not None:
                    self._tuples.remove(r)
                    return r
                self._condition.wait()

    def peek(self, t):
        while True:
            with self._condition:
                r = self._find(t)
                if r is not None:
                    return r
                self._condition.wait()

    def _find(self, t):
        return next((r for r in self._tuples if len(r) == len(t) and all(x == y or y is None for (x, y) in zip(r, t))), None)
