import subprocess
import time,socket  
from watchdog.observers import Observer  
from watchdog.events import PatternMatchingEventHandler
import os

def get_all_nodes():
    proc = subprocess.Popen(["swift-ring-builder","/etc/swift/object.ring.gz"], stdout=subprocess.PIPE)
    results = proc.stdout.read()
    lines = results.strip().split('\n')
    nodes = lines[4:]
    final_nodes = []
    for entry in nodes:
        stuff = entry.strip().split()
        final_nodes.append(str(stuff[3]))
    return final_nodes

def read_spindown():
    f = open("spindowndevices","r")
    contents = f.read()
    f.close()
    nodes = contents.strip().split("\n")
    sddict = dict()
    for i in nodes[1:]:
        if(i.split(":")[0] in sddict):
            sddict[i.split(":")[0]].append(i.split(":")[1])
        else:
            sddict[i.split(":")[0]] = []
            sddict[i.split(":")[0]].append(i.split(":")[1])
    return sddict,nodes


class MyHandler(PatternMatchingEventHandler):
    patterns = [r"*spindowndevices"] 
    def __init__(self):
        PatternMatchingEventHandler.__init__(self)
        self.count = 0
        print("In constructor")
    	
    def process(self, event):
        sddict,nodes = read_spindown() 
        myip = socket.gethostbyname(socket.gethostname())
        print myip
        print sddict
        if(nodes[0].strip().split("=")[1] == "True"):
        # flushing
            if(myip in sddict):
                for device in sddict[myip]:
                    print("===mounting===")
                    os.system("mount /dev/sdb1 /srv/node/"+str(device))
                      
                ## Unmount logic here after replication
                time.sleep(10) #waiting for replications to finish, rsync_timeout = 900
                new_sddict,new_nodes = read_spindown()
                for device in sddict[myip]:
                    os.system("umount /srv/node/"+str(device))
                    print("==unmount==")
        else:
            spinup_node = nodes[0].strip().split("=")[1].split(":")
            if spinup_node[0] == myip:
                os.system("mount /dev/sdb1 /srv/node/"+str(spinup_node[1]))
                
        #nodes = get_all_nodes()
        #print str(nodes)
        #for node in nodes:
        #    print("Rsync to:"+str(node))
        #    subprocess.call(["rsync","spindowndevices","swift@"+node+":spindowndevices"])

    def on_modified(self, event):
        self.count+=1
        print event.src_path
        print self.count
        #if self.count % 2 == 0:
        self.process(event)

if __name__ == '__main__':
    observer = Observer()
    observer.schedule(MyHandler(), path='.')
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
