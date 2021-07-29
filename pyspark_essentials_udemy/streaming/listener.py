import socket
import time

# setup the socket object
sock = socket.socket()
host = '127.0.0.1' 
port = 4203
sock.bind((host, port))

# talk
print('Listening on %s:%d'%(host, port))

# listen
sock.listen(5) # size of backlog of pending requests
clientSocket, address = sock.accept()

# talk
print('Received request from %s:%d, connected'%(address[0], address[1]))

# make the fake data & send it
waitSecs = 2
dataStream = ['test%d, '%i for i in range(5)]
for data in dataStream:
	print('Sending %s'%data)
	clientSocket.send(bytes(data, 'utf-8'))
	time.sleep(waitSecs)

# close the connection
clientSocket.close()
print('Closed connection')
