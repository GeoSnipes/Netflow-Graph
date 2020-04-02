"""Visualize the progress of network"""

#%%
from os import chdir, getcwd, listdir
import pandas as pd
import matplotlib.pyplot as plt
import datetime

try:
    rootdirectory = r'D:\Users\GV\Sync\PhD'
    chdir(rootdirectory)
except FileNotFoundError:
    rootdirectory = r'D:\Users\Geovanni\Sync\PhD'
    chdir(rootdirectory)

#%%
def converttimetodecimal(t):
    """Split time and convert to delta from midnight"""
    hhmmss = [x for x in t.split(':')]
    hhmmss[2], millisec = hhmmss[2].split('.')
    delta = datetime.timedelta(hours=int(hhmmss[0]), minutes=int(hhmmss[1]), seconds=int(hhmmss[2])) 
    delta = delta.seconds + int(millisec)/1000
    return delta

#%%
def calculateinterval(df_sort):
    global mintime
    #intervalstart = round(converttimetodecimal(df_sort.iloc[0].at['Time']), 3)    #Given a soreted frame, take first element as smallest time
    intervalstart = round(converttimetodecimal(mintime))
    intervalfinish = round(intervalstart + timeinterval - .001,3)
    # print(f'First entry:\n{df_sort.iloc[0]}\n{intervalstart}\n____________')
    
    intervals.append(0)
    trackflowbytesindex = 0     # use to keep track of current accumulation of bytes
    for index, row in df_sort.iterrows():
        convertedtime = converttimetodecimal(row['Time'])
        
        # Since it is sorted add a posiiton until current row is in correct interval bracket
        while convertedtime > intervalfinish:
            # print(convertedtime)
            # print(f"BEfore: {intervalstart}\t{intervalfinish}")
            intervalstart = round(intervalfinish + .001, 3)
            intervalfinish = round(intervalstart + timeinterval - .001, 3)
            trackflowbytesindex += 1
            # print(f"After: {intervalstart}\t{intervalfinish}")
        
        while convertedtime < intervalstart:
            print(row)
            print(convertedtime)
            print(f"BEfore: {intervalstart}\t{intervalfinish}")
            intervalfinish = round(intervalstart - .001,3)
            intervalstart = round(intervalfinish - timeinterval, 3)
            trackflowbytesindex -= 1
            # print(f"After: {intervalstart}\t{intervalfinish}")
            
        # Normal run if duration of current flow is within intervals
        if convertedtime >= intervalstart and convertedtime + float(row['Duration']) <= intervalfinish:
            try:
                flow_bytes[trackflowbytesindex] += row['Bytes']
            except IndexError:
                intervals.append(intervals[-1] + timeinterval)
                flow_bytes.append(0)
                flow_bytes[-1] += row['Bytes']
        else:
            # Exception run if duration of current flow is not within intervals
            continue
            print(f"{row['Time']}\t{row['Duration']}")
            print(f"{intervalstart}\t{intervalfinish}")
            print(f"{convertedtime}\t{convertedtime + float(row['Duration'])}")
        

    intervals.pop()
        
        


#%%
file = 'testVisual.csv'
mintime='23:59:59.999'
for df in pd.read_csv(file, usecols=['Time'], chunksize = 100000):
    if df['Time'].min() < mintime:
        print(df['Time'].min())
        mintime = df['Time'].min()


#%%
intervals = []
flow_bytes = []
timeinterval = 5


print(f'Working on file: {file}')
count =0
for df in pd.read_csv(file, usecols=['Time', 'Duration', 'Bytes'], chunksize = 100000):
    # df_necessary = df[['Time', 'Duration', 'Bytes']].copy()
    df.sort_values(by=['Time', 'Duration'], inplace=True)

    calculateinterval(df)
    

    # print(len(df.index))
    break
    # if count == 3:
    #     break
    # count += 1


#%%
plt.plot(intervals, flow_bytes)
plt.scatter(intervals, flow_bytes)

#%%
import datetime
