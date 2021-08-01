import tweepy 
from tweepy import OAuthHandler # to authenticate Twitter API
from tweepy import Stream 
from tweepy.streaming import StreamListener
import socket
import json
import pdb

def GetKeys(fileName):
	'''
	Load from a file the access and api keys needed, then return as a 
	list of access token, access secret, api key, api secret.
	'''
	# read the file and get the first 4 lines
	with open(fileName, 'rt') as f:
		keys = f.readlines()[:4]
	# drop the header in each row
	keys = [key.split(':')[1].strip() for key in keys]
	return keys

# handler class
class TweetsListener(StreamListener):
	def __init__(self, csocket):
		self.client_socket = csocket
		self.sep = '¬'

	def on_data(self, data):
		try:
			# read the incoming data ...
			msg = json.loads(data)
			#pdb.set_trace()
			msgTxt = msg['text']
			msgUser = msg['user']['screen_name']
			msgDat = msg['created_at']
			sendMe = (msgUser + self.sep + msgDat + self.sep + msgTxt).encode('utf-8')
			print(sendMe)
			# ... and send it
			self.client_socket.send(sendMe)
		except BaseException as err:
			print('Error: %s'%err)
		return True

	def on_error(self, status):
		print(status)
		return True

# ask user for topic to filter
topic = input('What topic to filter tweets?')

# get the keys
print('Getting access keys...')
try:
	keys = GetKeys('./keys.txt')
	print('Keys %s'%keys)
	accessToken, accessSecret, consumerKey, consumerSecret = keys
except Exception as err:
	print('Error getting keys: %s'%err)
	sock.close()
	print('Closed')
else:
	# setup the socket object
	sock = socket.socket()
	host = '127.0.0.1' 
	port = 4208
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
				twitterStream.filter(track=[topic])
			except Exception as err:
				print('Error: %s'%err)
				# close the connection
				clientSocket.close()
				sock.close()
				print('Closed connection')
