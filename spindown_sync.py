import subprocess
import time  
from watchdog.observers import Observer  
from watchdog.events import PatternMatchingEventHandler

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

class MyHandler(PatternMatchingEventHandler):
    patterns = [r"*spindowndevices"] 
    def __init__(self):
        PatternMatchingEventHandler.__init__(self)
        self.count = 0
        print("In constructor")
    	
    def process(self, event):
        nodes = get_all_nodes()
        print str(nodes)
        for node in nodes:
            print("Rsync to:"+str(node))
            subprocess.call(["rsync","spindowndevices","swift@"+node+":spindowndevices"])

    def on_modified(self, event):
        self.count+=1
        print event.src_path
        print self.count
        if self.count % 2 == 0:
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
