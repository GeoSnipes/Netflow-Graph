#flowInterval.py
import re
from sys import getsizeof
import time
import datetime

class NetFlow:

    def __init__(self, origFile, destFile, interval):
        self.origFile = origFile
        self.destFile = destFile
        self.interval = interval

    def __formatFile(self):
        """Remove extra spaces from original file and replace headings"""
        with open(self.origFile) as origF:
            origF.readline()                               #skip first line
            newF = open("formatted_nfcapd.txt", 'w')
            newF.write("Date_first_seen Time_first_seen Duration Proto Src_IP_Addr:Port Dir Dst_IP_Addr:Port Flags Tos Packets Bytes pps bps Bpp Flows\n") #fix first line titlw
            for line in origF:
                if (line.startswith("Summary:")):
                    break
                else:
                    line = self.__megGigtoBytes(line)
                    customLine = re.sub(r"\s{2,}", " ", line)      #regular expression to turn all multispace into one space
                    newF.write(customLine)                       #write output to new file
            newF.close()

    def __megGigtoBytes(self, line):
        """Some columns convert the data into mega/giga bytes, this fixes that by transforming it back into bytes"""
        expr = re.compile("\d+\.\d+\s{1}M")     #pattern for megabytes
        result = expr.findall(line)              #locate everywhere in the element that has the pattern
        if len(result) > 0:
            for found in result:
                num = int(float(found[:-2]) * 10 ** 6)
                line = line.replace(found, str(num))

        expr = re.compile("\d+\.\d+\s{1}G")  # pattern for gigabytes
        result = expr.findall(line)
        if len(result) > 0:
            for found in result:
                num = int(float(found[:-2]) * 10 ** 9)
                line = line.replace(found, str(num))

        return line


    def __getData(self):
        """time_since_epoch[0] Duration[1] Proto[2] Src_IP_Addr:Port[3] Dir[4] Dst_IP_Addr:Port[5] Flags[6] Tos[7] Packets[8] Bytes[9] pps[10] bps[11] Bpp[12] Flows[13]"""
        entries=[]
        with open("formatted_nfcapd.txt") as file:
            file.readline()                                 #skip the first line
            for line in file:
                entry = line.split(" ")              #split and return in the form of a list.
                entry = self.__formatDate(entry)            #Transform date and time into difference from epoch time
                entries.append(entry)             #This produces in a sense multidimensional array
        return entries

    def __formatDate(self, dateEntry):
        """Transform date and time into difference from epoch time"""
        date = dateEntry[0]+" "+dateEntry[1]
        try:
            dateFromd= datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            print(dateEntry)
            print(date)
        dateMicro= dateFromd.microsecond                    #timetuple() doesnt return microseconds, so have to save that seperately
        date = time.mktime(dateFromd.timetuple())+(dateMicro/10**6) #add microseconds to time
        del dateEntry[1]
        dateEntry[0] = date
        return dateEntry


    def __calInterval(self, data):
        finalSumList = []
        cumSum = 0
        start = data[0][0]
        finish = start + self.interval
        for i in range(len(data)):
            if data[i][0] <= finish:                          #value is less than finish
                try:
                    cumSum += int(data[i][9])
                except ValueError as e:
                    print(e)
                    print(data[i])
            else:                                           #current entry is not within current interval
                finalSumList.append(cumSum)                 #update final list of entries
                start = finish                               #update new interval
                finish += self.interval
                if start <= data[i][0] <= finish:  #if current entry is within new interval continue
                    try:
                        cumSum = int(data[i][9])
                    except ValueError as e:
                        print(e)
                        print(data[i])
                    continue
                else:
                    while not (start <= data[i][0] <= finish):           #if current entry is not within new interval then append 0 to final list until it true
                        finalSumList.append(0)
                        start = finish
                        finish += self.interval

                cumSum = int(data[i][9])

        finalSumList.append(cumSum)
        return finalSumList

    def __getFirst(self, elem):
        return elem[0]

    def __output_interval(self, intervalList, beginDataDate, endDataDate):
        val = self.interval
        output = open(self.destFile,"w")
        output.write(time.strftime("%x %X",time.localtime(beginDataDate))+"\t"+time.strftime("%x %X",time.localtime(endDataDate))+"\n")
        output.write(str(0)+"\t"+str(0)+"\n")
        for valInt in intervalList:
            output.write(str(val)+"\t"+str(valInt)+"\n")
            val += self.interval
        output.close

    def get_info(self):
        return "Source: {0} \tDestination: {1} \tInterval: {2}".format(self.origFile, self.destFile, self.interval)

    def run(self):
        self.__formatFile()
        data = self.__getData()
        data.sort(key=self.__getFirst)  # sort by first element in each sublist
        intervalList = self.__calInterval(data)
        self.__output_interval(intervalList, data[0][0], data[len(data) - 1][0])

origFile = ["netflowExtractedFiles/nfcapd.201801100000cswitch1.txt", "netflowExtractedFiles/nfcapd.201801100000cswitch2.txt", "netflowExtractedFiles/nfcapd.201801100000cswitch3.txt", "netflowExtractedFiles/nfcapd.201801100000dcdist.txt", "netflowExtractedFiles/nfcapd.201801100000igate.txt"]
finalOutput = ["netflowFinalised/cswitch1.201801100000.txt", "netflowFinalised/cswitch2.201801100000.txt", "netflowFinalised/cswitch3.201801100000.txt", "netflowFinalised/dcdist.201801100000.txt", "netflowFinalised/igate.201801100000.txt"]
#origFile = ["netflowExtractedFiles/testFile.txt"]
#finalOutput = ["netflowFinalised/testFile.txt"]
nF = []

interval = 5

startTimer = time.time()

for pos in range(0, len(origFile)):
    nF.append(NetFlow(origFile[pos], finalOutput[pos], interval))
    print(nF[pos].get_info())
    nF[pos].run()

stopTimer = time.time()
timer = int((stopTimer-startTimer)*1000)/1000

print("It took {0} seconds to complete.".format(timer))
