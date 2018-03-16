import csv
from os import chdir

chdir("D:\\Users\\Geovanni\\Sync\\Work\\PhD\\Netflow-Graph")

dic= {}

print('Length of ip list: '+ str(len(dic)))

reader = csv.reader(open('netflowExtractedFiles\\IpToNames.csv', 'r'))
for row in reader:
   k, v = row
   dic[k] = v

print('Length of ip list: ' + str(len(dic)))


with open("netflowFinalised\\igate.201802010000.csv", "r") as fileOut:
    file = fileOut.readlines()


newFile = []

for row in file:
    row = str.strip(row)
    row = row.split(",")
    sip = row[3].split(':')[0]
    dip = row[4].split(':')[0]
    # Source
    if False:
        if dic.__contains__(sip):
            row[3] = dic[sip]
            newFile.append(row)
    # Dest
    if False:
        if dic.__contains__(dip):
            row[4] = dic[dip]
            newFile.append(row)
    #_________OR_____________
    # Keep only ip addresses
    if False:
        row[3] = sip
        row[4] = dip
        newFile.append(row)


with open('netflowFinalised\\igate.201802010000final.csv', "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=',')
    csvwriter.writerows(newFile)
