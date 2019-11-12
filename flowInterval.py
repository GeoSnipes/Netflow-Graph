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
os.chdir("D:/Users/Username/Sync/Work/PhD/Netflow-Graph")


class NetFlow:

    def __init__(self, origFile, destFile, interval):
        self.origFile = origFile
        self.destFile = destFile
        self.interval = interval

    def __formatFile(self):
        """Remove extra spaces from original file and replace headings"""
        with open(self.origFile) as origF:
            origF.readline()  # skip first line
            newF = open("formatted_nfcapd.txt", 'w')
            newF.write(
                "Date_first_seen Time_first_seen Duration Proto Src_IP_Addr:Port Dir Dst_IP_Addr:Port Flags Tos Packets Bytes pps bps Bpp Flows\n")  # fix first line titlw
            for line in origF:
                if (line.startswith("Summary:")):
                    break
                else:
                    line = self.__megGigtoBytes(line)
                    customLine = re.sub(r"\s{2,}", " ",
                                        line)  # regular expression to turn all multispace into one space
                    newF.write(customLine)  # write output to new file
            newF.close()

    def __megGigtoBytes(self, line):
        """Some columns convert the data into mega/giga bytes, this fixes that by transforming it back into bytes"""
        expr = re.compile("\d+\.\d+\s{1}M")  # pattern for megabytes
        result = expr.findall(line)  # locate everywhere in the element that has the pattern
        if len(result) > 0:
            for found in result:
                num = int(float(found[:-2]) * 10 ** 6)
                line = line.replace(found, str(num))

        expr = re.compile("\d+\.\d+\s{1}G(?!RE)")  # pattern for gigabytes, ignoring when protocol is GRE
        result = expr.findall(line)
        if len(result) > 0:
            for found in result:
                num = int(float(found[:-2]) * 10 ** 9)
                line = line.replace(found, str(num))

        return line

    def __getData(self):
        """Get data from file and turn into a list. Also remove space from last element in each row and formate data to epoch time"""
        entries = []
        with open("formatted_nfcapd.txt") as file:
            file.readline()  # skip the first line
            for line in file:
                entry = line.split(" ")  # split and return in the form of a list.

                # Date[0] Time[1] Duration[2] Proto[3] Src_IP_Addr:Port[4] Dir[5] Dst_IP_Addr:Port[6] Flags[7] Tos[8] Packets[9] Bytes[10] pps[11] bps[12] Bpp[13] Flows[14]
                entry[14] = entry[14].strip()  # get rid of new line at end
                entry = self.__formatDate(entry)  # Transform date and time into difference from epoch time

                # time_since_epoch[0] Duration[1] Proto[2] Src_IP_Addr:Port[3] Dir[4] Dst_IP_Addr:Port[5] Flags[6] Tos[7] Packets[8] Bytes[9] pps[10] bps[11] Bpp[12] Flows[13]
                entries.append(entry)  # This produces in a sense multidimensional array
        return entries

    def __formatDate(self, dateEntry):
        """Transform date and time into difference from epoch time"""
        date = dateEntry[0] + " " + dateEntry[1]
        try:
            dateFromd = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            print(dateEntry)
            print(date)
        dateMicro = dateFromd.microsecond  # timetuple() doesnt return microseconds, so have to save that seperately
        date = time.mktime(dateFromd.timetuple()) + (dateMicro / 10 ** 6)  # add microseconds to time
        del dateEntry[1]
        dateEntry[0] = date
        return dateEntry

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

    def __getPort(self, row):
        """Get either address, port, or both and remove dir, flags, pps, bps, Bpp, and flows"""
        if True:
            """Keep address, no ports"""
            port = row[3].split(':')
            row[3] = port[0]
            port = row[5].split(':')
            row[5] = port[0]
        elif False:
            """Keep ports only"""
            port = row[3].split(':')
            row[3] = port[1]
            port = row[5].split(':')
            row[5] = port[1]
        del (row[-4:])
        del (row[-4])
        del (row[-5])
        return row

    def run(self):
        self.__formatFile()
        data = self.__getData()
        # Change protocol types to numbers
        if False:  # Labelencoder/Onehotencoder does this properly, no longer needed
            for row in data:
                if row[2] == 'TCP':
                    row[2] = 1
                elif row[2] == 'UDP':
                    row[2] = 2
                elif row[2] == 'EIGRP':
                    row[2] = 3
                elif row[2] == 'ESP':
                    row[2] = 4
                elif row[2] == 'GRE':
                    row[2] = 5
                elif row[2] == 'ICMP':
                    row[2] = 6
                elif row[2] == 'IGMP':
                    row[2] = 7
                elif row[2] == 'IPv6':
                    row[2] = 8
                elif row[2] == 'L2TP':
                    row[2] = 9
                else:
                    row[2] = 0

        # update to save as csv, instead of full ipaddressm keep only ports (future implementation- not finished)
        with open(self.destFile[:-3] + "csv", "w", newline="") as csvfile:
            if False:
                """Use only ports of each address"""
                data2 = list(map(self.__getPort, copy.deepcopy(data)))
                csvwriter = csv.writer(csvfile, delimiter=',')
                csvwriter.writerow(["EpochTime", "Duration", "Protocol", "Src", "Dst", "Tos", "Packets", "Bytes"])
                csvwriter.writerows(data2)
                del data2
            else:
                """Copy to file as is"""
                csvwriter = csv.writer(csvfile, delimiter=',')
                csvwriter.writerow(
                    ["EpochTime", "Duration", "Protocol", "Src", "Dir", "Dst", "Flags", "Tos", "Packets", "Bytes",
                     "pps", "bps", "Bpp", "Flows"])
                csvwriter.writerows(data)

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
