
from pyspaces import PySpaceShMem

def main():
    with PySpaceShMem() as space:
        space.put(('hello', 'world'))
        print(space.take((None, None)))

if __name__ == '__main__':
    main()
