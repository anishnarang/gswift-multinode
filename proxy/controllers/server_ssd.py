import socket
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

import logging
logging.basicConfig(filename='/home/swift/swift.log',level=logging.DEBUG)

def check(s):
	cmd = "df -h "+s
	logging.info(cmd)
	output = commands.getoutput(cmd)
	output=output.split()
	#x=os.system("df -h /media/hduser/UUI/")
	logging.info(output)
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
	node_ip = rsync_ip(device['ip'])
	rsync_module = 'swift@%s:' %(node_ip)
	spath = '/SSD/'+str(partition)
	args = [
            'rsync',
            '--recursive',
            '--whole-file',
            '--human-readable',
            '--xattrs',
            '--itemize-changes',
            '--ignore-existing',
            '--timeout=30',
            '--archive'
    ]
	args.append(spath)
	# args.append(join(rsync_module,'srv/node',device['device'],'objects',partition))
	args.append(join(rsync_module,'srv/node',device['device'],'objects/'))
	logging.info("===args===%s",str(args))
	start_time = time.time()
	ret_val = None
	try:
		with Timeout(900):
			proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
			results = proc.stdout.read()
			ret_val = proc.wait()

	except Timeout:
		logging.info("Killing long-running rsync: %s", str(args))
		proc.kill()
		return 1
	total_time = time.time() - start_time
	logging.info("===Total time===%s",str(total_time))
	for result in results.split('\n'):
	    if result == '':
	        continue
	    if result.startswith('cd+'):
	        continue
	if not ret_val:
		logging.info(result)
	else:
		logging.info(result)
	if ret_val:
		logging.info('Bad rsync return code: %(args)s -> %(ret)d',
	                      {'args': str(args), 'ret': ret_val})
	elif results:
		logging.info("Successful rsync of %(src)s at %(dst)s (%(time).03f)",
	        {'src': args[-2], 'dst': args[-1], 'time': total_time})
	else:
		logging.info("Successful rsync of %(src)s at %(dst)s (%(time).03f)",
	        {'src': args[-2], 'dst': args[-1], 'time': total_time})

def flush(sddict):
	# dict_info = pickle.load(open('/usr/bin/device.p','rb'))
	## Should nodes.txt still have part:node? Or should it have hash:node?
	## WHat if the destination directory of flush already has that partition folder?

	dict_info = ast.literal_eval(open("/home/swift/nodes.txt","r").read())
	files = [f for f in listdir('/SSD')] 
	logging.info("===In flush===")
	# for f in files:
	# 	logging.info("===FILE in SSD===",f)
	# 	deviceList = dict_info[int(f)]
	# 	for i in deviceList:
	# 		os.system('mkdir -p /srv/node/%s/objects/' %(i['device']))
	# # os.system('chown -R swift:swift /srv/node')
	for f in files:
		deviceList = dict_info[int(f)]
		for i in deviceList:
			logging.info("===I===%s",str(i))
			logging.info("===sddict===%s",str(sddict))
			if(i['ip'] not in sddict or i['device'] not in sddict[i['ip']]):	## MEans its mounted
	            		rsync(f,i)
				logging.info("===Mouted===")
	        	else:
				logging.info("===In else===")
				rsync(f,i)
	        	#os.system("ssh swift@%s echo 'password' | sudo -S mount /dev/%s",(i['ip'],i['device']))
	        	#rsync(f,i)
	        	#os.system("ssh swift@%s echo 'password' | sudo -S umount /dev/%s",(i['ip'],i['device']))
	## Delete all or use cache
	#os.system("cd /SSD/")
	#os.system("rm -rf *")
	        	
	#logging.info("flush done.... deleting dictionary")
	#del_dict()


def check_ssd():
	# serv = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	# serv.bind(('10.10.1.90',8889))
	# serv.listen(20)
	# while True:
	# 	#logging.info "in while"
	# 	conn, addr = serv.accept()
	# 	logging.info("Got connection.")
	# 	#logging.info "after accept"

	# 	data = conn.recv(512)
	# 	# logging.info data
	# 	if data == 'start':
	f = open('/home/swift/spindowndevices','r')
	s = f.read()
	sdlist = s.strip().split('\n')
	f.close()

	sddict = dict()
	for i in sdlist:
    		if(i.split(":")[0] in sddict):
        		sddict[i.split(":")[0]].append(i.split(":")[1])
    		else:
        		sddict[i.split(":")[0]] = []
 	       		sddict[i.split(":")[0]].append(i.split(":")[1])
	
	perc = check('/SSD/')
	logging.info("===SSD Percentage===%s",perc)
	## Call flush if SSD is full
	flush(sddict)
