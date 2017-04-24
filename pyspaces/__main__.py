'''
this module is the entry point for the tuple spaces server.
'''

import argparse
from pyspaces import PySpaceServer


def main():
    '''
    entry point - invoked when run from the command line.
    '''
    parser = argparse.ArgumentParser(description='pyspaces http server')
    parser.add_argument('--listen', action='store', default='127.0.0.1',
            help='the host to listen on for pyspaces client connections')
    parser.add_argument('--port', action='store', default=8888, type=int,
            help='the port to listen on for pyspaces client connections')

    args = parser.parse_args()
    server = PySpaceServer(args.listen, args.port)
    server.serve_forever()


if __name__ == '__main__':
    main()
