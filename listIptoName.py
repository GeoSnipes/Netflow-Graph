from os import chdir, path
import subprocess
import csv
import queue
import threading
from ipaddress import ip_address as checkIP
import time

chdir("D:\\Users\\Geovanni\\Sync\\Work\\PhD\\Netflow-Graph\\netflowExtractedFiles")
# chdir("D:\\Users\\Geovanni\\Sync\\Work\\PhD\\")

Name = {}
file_list = [
    "nfcapd.201802010000.txt",
    "nfcapd.201802010005.txt",
    "nfcapd.201802010010.txt",
    "nfcapd.201802010015.txt",
    "nfcapd.201802010020.txt",
    "nfcapd.201802010025.txt",
    "nfcapd.201802010030.txt",
    "nfcapd.201802010035.txt",
    "nfcapd.201802010040.txt",
    "nfcapd.201802010045.txt",
    "nfcapd.201802010050.txt"
]
# file_list2 = ["testFile.txt"]
# file_list2 = ["testFile.txt", "testFile2.txt", "testFile3.txt"]
file_list2 = ["nfcapd.201802010000.txt"]

exitFlag = 0

class myThread(threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q

    def run(self):
        print("Starting " + self.name)
        process_data(self.name, self.q)
        print("Exiting " + self.name)


def process_data(threadName, q):
    while not exitFlag:
        queueLock.acquire()
        if not workQueue.empty():
            data = q.get()
            queueLock.release()
            formatIPLine(data)
        else:
            queueLock.release()


def formatIPLine(line):
    formatLine = line.split(maxsplit=7)

    # Source address
    ip = formatLine[4].split(":")[0]
    try:
        if not checkIP(ip).is_private:
            if ip not in Name:
                formatOut(subprocess.run("nslookup " + ip, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout, ip)
        else:
            if ip not in Name:
                formatOut(b'UMKC', ip)
    except ValueError:
        pass

    # Destination address
    ip = formatLine[6].split(":")[0]
    try:
        if not checkIP(ip).is_private:
            if ip not in Name:
                formatOut(subprocess.run("nslookup " + ip, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout, ip)
        else:
            if ip not in Name:
                formatOut(b'UMKCPrivate', ip)
    except ValueError:
        pass


def formatOut(console, ip):
    console = console.decode("utf-8")
    if str(console).find("Name:") != -1:
        Name[ip] = console.split("\r\n\r\n")[1].split("\r\n")[0].split(":")[1].strip()
    elif console == "UMKCPrivate":
        Name[ip] = "UMKCPrivate"
    elif (ip.startswith('134.193.')):
        Name[ip] = ip
    else:
        Name[ip] = "Unknown"


threadList = []
for i in range(1, 500):
    threadList.append("Thread-" + str(i))

queueLock = threading.Lock()
workQueue = queue.Queue(0)
threads = []
threadID = 1
entFile = []

# Print the length of each file
print(len(entFile))
for eachFile in file_list2:
    with open(eachFile, "r") as file:
        file.readline()
        entFile += file.readlines()
        lenentFile = len(entFile)
        print(lenentFile)

#Load current ip to names file into dictionary
if path.exists('IpToNames.csv'):
    print('Checking current list of ip to names')
    print('Length of ip list: '+ str(len(Name)))
    reader = csv.reader(open('IpToNames.csv', 'r'))
    for row in reader:
       k, v = row
       Name[k] = v

    print('Length of ip list: ' + str(len(Name)))

startT = time.time()

# Create new threads
for tName in threadList:
    thread = myThread(threadID, tName, workQueue)
    thread.start()
    threads.append(thread)
    threadID += 1

# Fill the queue
queueLock.acquire()
for line in entFile:
    workQueue.put(line)
queueLock.release()

# Wait for queue to empty
while not workQueue.empty():
    pass

# Notify threads it's time to exit
exitFlag = 1

# Wait for all threads to complete
for t in threads:
    t.join()

endT = time.time()

print("It took {} seconds to nslookup all {} records".format(endT - startT, lenentFile))

with open('IpToNames.csv', 'w', newline="") as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=',')
    for key, value in Name.items():
        csvwriter.writerow([key, value])
