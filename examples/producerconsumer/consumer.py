
import pyspaces

p = pyspaces.PySpace('http://localhost:10000')

while(True):
    print(p.take(('q', None)))
