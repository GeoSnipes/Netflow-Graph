from os import chdir, path
import os
import subprocess
import queue
import threading
from ipaddress import ip_address as checkIP
import pandas as pd

import Settings

class myThread(threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.sample = q

    def run(self):
        print("Starting " + self.name)
        process_data(self.name, self.sample)
        

def process_data(threadName, sample):
    global exitFlag
    global hostnames
    for ip in sample:
        if exitFlag:
            break
        if ip in hostnames:
            continue
        data = get_hostname(ip)
        queueLock.acquire()
        hostnames[ip]=data
        queueLock.release()

def get_hostname(ip):
    if (ip.startswith('134.193.')):
        return 'UMKC'
    elif not checkIP(ip).is_private:
        return formatOut(subprocess.run(f"nslookup {ip}", capture_output=True).stdout)
    else:
        return 'UMKCPRIVATE'
    

def formatOut(console):
    console = console.decode("utf-8")
    if str(console).find("Name:") != -1:
        return console.split("\r\n\r\n")[1].split("\r\n")[0].split(":")[1].strip()
    else:
        return "Unknown"

def group(lst, n):
  for i in range(0, len(lst), n):
    val = lst[i:i+n]
    if len(val) == n:
      yield tuple(val)

DATA_DIREC = Settings.NETFLOW_PROS
file_list = os.listdir(DATA_DIREC)

queueLock = threading.Lock()
workQueue = queue.Queue(0)
threads = []
entFile = []

print('Collectin previously found lookups')
hostnames = pd.read_feather('./feather_hostnames')
hostnames = dict([(ip, host) for ip, host in zip(hostnames.ip_addr, hostnames.hostnames)])

exitFlag = 0

threadList = []
for i in range(1, 200):
    threadList.append("Thread-" + str(i))

df= None
files = os.listdir('./Data/Conv')
if len(files)%5 != 0:
    files.extend(['_']*(5-len(files)%5))

# TODO: Use queue
try:
    for file_groups in list(group(files, 5)):
        df = pd.DataFrame()
        for conv_file in file_groups:
            if conv_file == '_':
                continue
            if df is None:
                df = pd.read_feather(DATA_DIREC+'/'+conv_file)
            else:
                df = pd.concat([df, pd.read_feather(DATA_DIREC+'/'+conv_file)], ignore_index=True)
        
        if df.shape[0] <= 0:
            continue

        ips = list(set(df['Src_IP_Addr']).union(set(df['Dst_IP_Addr'])))
        start = 0
        width = 50
        for x in range(0, len(ips),width):
            end = start + width
            thread = myThread(start, f'Thread-{start}:{end}', ips[start:end])
            thread.start()
            threads.append(thread)
            start=end

            if len(threads) > 100:
                print('[INFO] 100 threads, waiting.....')
                for t in threads:
                    t.join()
                    print("Exiting " + t.name)
                threads=[]

except KeyboardInterrupt:
    exitFlag=1

print('[INFO] Saving file')
pd.DataFrame({'ip_addr':list(hostnames.keys()), 'hostnames':list(hostnames.values())}).to_feather('./feather_hostnames')
