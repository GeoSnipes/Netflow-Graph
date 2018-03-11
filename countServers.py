import csv
from os import chdir

chdir("D:\\Users\\Geovanni\\Sync\\Work\\PhD\\Netflow-Graph")

dic = {}
with open("netflowFinalised\\igate.201802010000editfinal.csv", "r") as fileIn:
    file = fileIn.readlines()

for row in file:
    row = str.strip(row)
    row = row.split(",")
    if dic.__contains__(row[3]):
        dic[row[3]] += 1
    else:
        dic[row[3]] = 1

with open("netflowFinalised\\igate.201802010000count.csv", "w") as fileOut:
    for key, value in dic.items():
        fileOut.write(key+","+str(value)+"\n")