import re
import os

os.chdir('D:\\Users\\'Username'\\Sync\\Work\\PhD\\Netflow-Graph')

with open("formatted_DST_UMKC.csv") as fileIN:
    f = fileIN.read()
    doc = re.sub(r"(?<=:\d\d\d)\.\d", "", f)
    doc = re.sub(r"(?<=:\d\d)\.\d", "", f)
    doc = re.sub(r"(?<=:\d)\.\d", "", f)
    doc = re.sub(r"(?<=:11)\.0", "", f)
    with open("fUMKC.csv","w") as fileOUT:
        fileOUT.write(doc)
