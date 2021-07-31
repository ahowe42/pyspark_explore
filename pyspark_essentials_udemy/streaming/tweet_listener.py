import tweepy 
from tweepy import OAuthHandler # to authenticate Twitter API
from tweepy import Stream 
from tweepy.streaming import StreamListener
import socket
import json
import getpass


# handler class
class TweetsListener(StreamListener):
	def __init__(self, csocket):
		self.client_socket = csocket
	def onData(self, data):
		try:
			msgTxt = json.loads(data) 
			msgTxt = msg['text'].encode('utf-8')
			print(msgTxt)
			self.client_socket.send(msgTxt)
		except BaseException as err:
			print('Error: %s'%err)
		# done
		return True
 
	def onError(self, status):
		print(status)
		return True

# setup the socket object
sock = socket.socket()
host = '127.0.0.1' 
port = 4205
sock.bind((host, port))
print('Listening on %s:%d'%(host, port))

# listen
try:
	sock.listen(5) # size of backlog of pending requests
	clientSocket, address = sock.accept()	
except KeyboardInterrupt as err:
	sock.close()
	print('Closed')
else:
	print('Received request from %s:%d, connected'%(address[0], address[1]))

	# get the tokens and setup twitter authorization
	accessToken = getpass.getpass('Access Token')
	accessSecret = getpass.getpass('Access Secret')
	consumerKey = getpass.getpass('API Key')
	consumerSecret = getpass.getpass('API Secret')
	try:
		auth = OAuthHandler(consumerKey, consumerSecret)
		auth.set_access_token(accessToken, accessSecret)
	except Exception as err:
		print('Authorization error: %s'%err)
		clientSocket.close()
		sock.close
	else:
		try:
			# listen to twitter, but only partially listen
			twitterStream = Stream(auth, TweetsListener(clientSocket))
			twitterStream.filter(track=['NASA'])
		except Exception as err:
			print('Error: %s'%err)
			# close the connection
			clientSocket.close()
			sock.close()
			print('Closed connection')
