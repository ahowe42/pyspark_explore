import socket
import time

# setup the socket object - connecting to the listener
sock = socket.socket()
host = '127.0.0.1' 
port = 4203
sock.connect((host, port))

# talk
print('Connected to %s:%d'%(host, port))

# setup receiving - note the infinite loop
waitSecs = 5
try:
	while True:
		msg = sock.recv(1024).decode('utf-8') # I guess this is number of bytes?
		print(msg)
		time.sleep(waitSecs)
except KeyboardInterrupt:
	sock.close()
