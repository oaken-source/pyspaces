
import pyspaces
import time

p = pyspaces.PySpace('http://localhost:10000')

i = 0

while(True):
    p.put(('q', i))
    i += 1
    time.sleep(1)
