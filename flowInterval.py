# flowInterval.py
import os
import re
import time
import datetime
import csv
from math import floor, ceil
from decimal import Decimal, getcontext
import copy

# Path to netflow data
os.chdir("D:/Users/Geovanni/Sync/Work/PhD/Netflow-Graph")


class NetFlow:

    def __init__(self, origFile, destFile, interval):
        self.origFile = origFile
        self.destFile = destFile
        self.interval = interval

    def __format_long_transmits(self, data):
        """For transfer that last longer than the intervals"""
        getcontext().prec = 4
        beg = 0  # used to save processing time, instead of going over list each time, start at point where list ends
        allChecked = True
        newData = []  # save new flows that last longer than interval
        while allChecked:  # for does not iterate through an ever updating list, so have to refresh the iteration counter
            allChecked = False
            for pos in range(beg, len(data)):
                beg += 1
                if float(data[pos][1]) > self.interval:
                    allChecked = True  # then there is a flow that last longer than interval
                    flowDuration = data[pos][1]
                    tempFlow = list(data[pos])
                    data[pos][1] = str(
                        self.interval)  # if flowDuration was greater than interval, then set it to interval
                    for val in range(8, 13):  # calculate ratio of val:flowDuration
                        if val != 10:
                            data[pos][val] = str(floor(float(data[pos][val]) / float(flowDuration) * self.interval))
                        else:
                            data[pos][val] = str(ceil(
                                float(data[pos][val]) / float(flowDuration) * self.interval))  # can't have 0 packets
                    self.__remain(tempFlow, data[pos])
                    newData.append(tempFlow)

            data = data + newData
            newData = []
        return data

    def __remain(self, tempFlow, flow):
        """Calculate remaining data from interval-ed original"""
        tempFlow[0] = tempFlow[0] + self.interval
        tempFlow[1] = str(float(Decimal(tempFlow[1]) - Decimal(self.interval)))
        for pos in range(8, 13):
            tempFlow[pos] = str(ceil(Decimal(tempFlow[pos]) - Decimal(flow[pos])))

    def __cal_interval(self, data):
        """Summation of each interval bytes transfered"""
        finalSumList = []
        cumSum = 0
        start = data[0][0]
        finish = start + self.interval
        for i in range(len(data)):
            if data[i][0] < finish:  # value is less than finish
                try:
                    cumSum += int(data[i][9])
                except ValueError as e:
                    print(e)
                    print(data[i])
            else:  # current entry is not within current interval
                finalSumList.append(cumSum)  # update final list of entries
                start = finish  # update new interval
                finish += self.interval
                if start <= data[i][0] < finish:  # if current entry is within new interval continue
                    try:
                        cumSum = int(data[i][9])
                    except ValueError as e:
                        print(e)
                        print(data[i])
                    continue
                else:
                    while not (start <= data[i][
                        0] < finish):  # if current entry is not within new interval then append 0 to final list until it true
                        finalSumList.append(0)
                        start = finish
                        finish += self.interval

                cumSum = int(data[i][9])

        finalSumList.append(cumSum)
        return finalSumList

    def __get_first(self, elem):
        """Used to sort by first element(date) in list"""
        return elem[0]

    def __output_interval(self, intervalList, beginDataDate, endDataDate):
        """Create final output"""
        # output = open(self.destFile, "w")
        # output.write(time.strftime("%x %X", time.localtime(beginDataDate)) + "\t" + time.strftime("%x %X",
        #                                                                                           time.localtime(
        #                                                                                               endDataDate)) + "\n")
        # output.write(time.strftime("%x %X", time.localtime(beginDataDate)) + "\t" + str(0) + "\n")
        # beginDataDate += self.interval
        # for valInt in intervalList:
        #     output.write(time.strftime("%x %X", time.localtime(beginDataDate)) + "\t" + str(valInt) + "\n")
        #     beginDataDate += self.interval
        # output.close

        with open(self.destFile[:-4] + "INTERVAL.csv", "w", newline="") as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',')
            csvwriter.writerow(['Date', 'Data_Transfered'])
            csvwriter.writerow([time.strftime("%x %H:%M:%S", time.localtime(beginDataDate)), str(0)])
            beginDataDate += self.interval
            for valInt in intervalList:
                csvwriter.writerow([time.strftime("%x %H:%M:%S", time.localtime(beginDataDate)), str(valInt)])
                beginDataDate += self.interval

    def get_info(self):
        return "Source: {0} \tDestination: {1} \tInterval: {2}".format(self.origFile, self.destFile, self.interval)

    def run(self):
        data = self.__format_long_transmits(data)
        data.sort(key=self.__get_first)  # sort by first element in each sublist
        intervalList = self.__cal_interval(data)
        self.__output_interval(intervalList, data[0][0], data[len(data) - 1][0])


origFile = [
    "netflowExtractedFiles/nfcapd.201802010000.txt",
    "netflowExtractedFiles/nfcapd.201802010005.txt",
    "netflowExtractedFiles/nfcapd.201802010010.txt",
    "netflowExtractedFiles/nfcapd.201802010015.txt",
    "netflowExtractedFiles/nfcapd.201802010020.txt",
    "netflowExtractedFiles/nfcapd.201802010025.txt",
    "netflowExtractedFiles/nfcapd.201802010030.txt",
    "netflowExtractedFiles/nfcapd.201802010035.txt",
    "netflowExtractedFiles/nfcapd.201802010040.txt",
    "netflowExtractedFiles/nfcapd.201802010045.txt",
    "netflowExtractedFiles/nfcapd.201802010050.txt"
]
finalOutput = [
    "netflowFinalised/igate.201802010000.txt",
    "netflowFinalised/igate.201802010005.txt",
    "netflowFinalised/igate.201802010010.txt",
    "netflowFinalised/igate.201802010015.txt",
    "netflowFinalised/igate.201802010020.txt",
    "netflowFinalised/igate.201802010025.txt",
    "netflowFinalised/igate.201802010030.txt",
    "netflowFinalised/igate.201802010035.txt",
    "netflowFinalised/igate.201802010040.txt",
    "netflowFinalised/igate.201802010045.txt",
    "netflowFinalised/igate.201802010050.txt"
]
# origFile = ["netflowExtractedFiles/testFile.txt"]
# finalOutput = ["netflowFinalised/testFile.txt"]
nF = []

interval = 5

startTimer = time.time()

for pos in range(len(origFile)):
    nF.append(NetFlow(origFile[pos], finalOutput[pos], interval))
    print(nF[pos].get_info())
    nF[pos].run()

stopTimer = time.time()
timer = int((stopTimer - startTimer) * 1000) / 1000

print("It took {0} seconds to complete.".format(timer))
