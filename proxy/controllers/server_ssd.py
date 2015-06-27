import socket
#from flush_ssd import *
# from chkdisk import *
import commands
import os
import ast
from os import listdir
from os.path import basename, dirname, isdir, isfile, join
import random
import time
import cPickle as pickle
from eventlet import Timeout
from eventlet.green import subprocess
from swift.common.utils import rsync_ip


def check(s):
	cmd = "df -h "+s
	print(cmd)
	output = commands.getoutput(cmd)
	output=output.split()
	#x=os.system("df -h /media/hduser/UUI/")
	print(output)
	x=output[11].strip("%")
	return int(x)


def del_dict():
	f = pickle.load(open('/usr/bin/device.p','rb'))
	f= {}
	pickle.dump(f,open('/usr/bin/device.p','wb'))

	f = open("nodes.txt","r")	# Must contain {} if empty
	d = f.read()
	d = "{}"
	f.close()
	f = open("nodes.txt","w")
	f.write(str(d))
	f.close()


def rsync(partition, device):
	node_ip = rsync_ip('127.0.0.1')
	rsync_module = '%s::object' %(node_ip)
	spath = join('/SSD/%s' %partition)
	args = [
            'rsync',
            '--recursive',
            '--whole-file',
            '--human-readable',
            '--xattrs',
            '--itemize-changes',
            '--ignore-existing',
            '--timeout=30'
    ]
	args.append(spath)
	# args.append(join(rsync_module,'srv/node',device['device'],'objects',partition))
	args.append(join(rsync_module,'/srv/node',device['device'],'objects',partition))
	start_time = time.time()
	ret_val = None
	try:
		with Timeout(900):
			proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
			results = proc.stdout.read()
			ret_val = proc.wait()

	except Timeout:
		print("Killing long-running rsync: %s", str(args))
		proc.kill()
		return 1
	total_time = time.time() - start_time

	for result in results.split('\n'):
	    if result == '':
	        continue
	    if result.startswith('cd+'):
	        continue
	if not ret_val:
		print(result)
	else:
		print(result)
	if ret_val:
		print('Bad rsync return code: %(args)s -> %(ret)d',
	                      {'args': str(args), 'ret': ret_val})
	elif results:
		print("Successful rsync of %(src)s at %(dst)s (%(time).03f)",
	        {'src': args[-2], 'dst': args[-1], 'time': total_time})
	else:
		print("Successful rsync of %(src)s at %(dst)s (%(time).03f)",
	        {'src': args[-2], 'dst': args[-1], 'time': total_time})

def flush():
	# dict_info = pickle.load(open('/usr/bin/device.p','rb'))
	dict_info = ast.literal_eval(open("/home/swift/nodes.txt","r").read())
	files = [f for f in listdir('/SSD')] 
	for f in files:
		print("===FILE in SSD===",f)
		deviceList = dict_info[int(f)]
		for i in deviceList:
			os.system('mkdir -p /srv/node/%s/objects/' %(i['device']))
	# os.system('chown -R swift:swift /srv/node')
	for f in files:
	     deviceList = dict_info[int(f)]
	     # suffix = listdir('/mnt/SSD')
	     for i in deviceList:
	            # rsync(f,i,suffix)
	            rsync(f,i)
	# print "flush done.... deleting dictionary"
	# del_dict()


def main():
	f = open('/home/swift/spindowndevices','r')
	s = f.read()
	list_device = s.split('\n')
	list_device.remove('')
	f.close()
	serv = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	serv.bind(('localhost',8889))
	serv.listen(20)
	while True:
		#print "in while"
		conn, addr = serv.accept()
		print("Got connection.")
		#print "after accept"
		data = conn.recv(512)
		# print data
		if data == 'start':
			perc = check('/SSD/')
			print("===SSD Percentage===",perc)
			## Call flush if SSD is full
			# flush()

		 	# if perc > 85:
				# # for i in list_device:
				# # 	print i
				# # os.system('mount -t xfs -L %s /srv/node/%s'%(i,i))	
			 #    #flush()
			 #    # for i in range(ord('d'),ord('g')):
				# # os.system('umount /dev/sd%s'%(chr(i)))
		conn.send('done')


if __name__ == '__main__':
	 main()
