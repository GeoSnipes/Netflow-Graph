"""count the number of times the address appears"""
import csv
from os import chdir

chdir("D:\\Users\\'Username'\\Sync\\Work\\PhD\\sample")

dic = {}
with open("igate_data.csv", "r") as fileIn:
    file = fileIn.readlines()

for row in file:
    row = str.strip(row)
    row = row.split(",")
    if dic.__contains__(row[3]):
        dic[row[3]] += 1
    else:
        dic[row[3]] = 1

with open("igate_count.csv", "w") as fileOut:
    sorted_data = sorted(dic, key=dic.__getitem__, reverse=True)
    for key in sorted_data:
         fileOut.write(key+","+str(dic[key])+"\n")
    # for key, value in dic.items():
    #     fileOut.write(key+","+str(value)+"\n")
