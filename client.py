import sys
import pickle
import gevent

from gevent import socket


data = pickle.dumps({'value': int(sys.argv[2])})

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.sendto(data, 0, ('127.0.0.1', int(sys.argv[1])))

data, addr = s.recvfrom(1024)
print(pickle.loads(data))
