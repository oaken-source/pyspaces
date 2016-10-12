'''
this module is the entry point for the tuple spaces server.
'''

from pyspaces import PySpaceServer


def main():
    '''
    entry point - invoked when run from the command line.
    '''
    server = PySpaceServer('localhost', 10000)
    server.serve_forever()


if __name__ == '__main__':
    main()
