import csv
from os import chdir

chdir("D:\\Users\\Geovanni\\Sync\\Work\\PhD\\Netflow-Graph")

dic= {}
with open("netflowExtractedFiles\\capd1Edited.csv") as listIptoName:
    file = listIptoName.readlines()

for row in file:
    row = str.strip(row)
    row = row.split(",")
    dic[row[0]] = row[1]

with open("netflowFinalised\\igate.201802010000edit.csv", "r") as fileOut:
    file = fileOut.readlines()

newFile = []

for row in file:
    row = str.strip(row)
    row = row.split(",")
    if dic.__contains__(row[3]):
        row[3] = dic[row[3]]
        newFile.append(row)

with open('netflowFinalised\\igate.201802010000editfinal.csv', "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=',')
    csvwriter.writerows(newFile)
