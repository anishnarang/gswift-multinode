import socket
import os

def check_ssd():
	client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	client.connect(('localhost',8889))
	# print "connected"
	client.send('start')
	# print "after send"
	data = client.recv(512)
	# print data
	client.close()
	return 1

#check_ssd()
