'''Convert raw nfdump file into process format'''
#%%
import os, time
import pandas as pd
from multiprocessing import Pool, cpu_count
SOURCE = './Data/Conv'

# %%
def process_nfdump(filename: str) -> pd.DataFrame:
    if filename.endswith('_conv'):
        # TODO: check whether the file has already been converted
        print(f'Started on {filename}')
        start = time.time()

        df = pd.read_fwf(SOURCE+'/'+filename).iloc[:-5,:]
        df.columns = ['D_firstseen', 'T_firstseen', 'Duration', 'Protocol', 'Src_IP_Addr:Port', 'Direc', 'Dst_IP_Addr:Port', 'Flags', 'Tos', 'Packets', 'Bytes', 'Flows']
        
        # retrive ports
        df['Src_Port']=df['Src_IP_Addr:Port'].apply(lambda x: x.split(':')[1])
        df['Dst_Port']=df['Dst_IP_Addr:Port'].apply(lambda x: x.split(':')[1])
        
        # restructure to just ip address
        df['Src_IP_Addr:Port']=df['Src_IP_Addr:Port'].apply(lambda x: x.split(':')[0])
        df['Dst_IP_Addr:Port']=df['Dst_IP_Addr:Port'].apply(lambda x: x.split(':')[0])
        
        # combine to date and time
        df['D_firstseen'] = df['D_firstseen'] +' '+df['T_firstseen']
        df = df[['D_firstseen', 'Duration', 'Protocol', 'Src_IP_Addr:Port', 'Src_Port','Direc', 'Dst_IP_Addr:Port', 'Dst_Port','Flags', 'Tos', 'Packets', 'Bytes', 'Flows']]
        df.columns = ['Datetime_firstseen', 'Duration', 'Protocol', 'Src_IP_Addr', 'Src_Port','Direc', 'Dst_IP_Addr', 'Dst_Port','Flags', 'Tos', 'Packets', 'Bytes', 'Flows']

        # some files have 'M' or 'G' suffix, convert to float
        df['Bytes'] = df['Bytes'].apply(convert_prefix_tonum)
        df['Packets'] = df['Packets'].apply(convert_prefix_tonum)

        # set column types
        col_types = {'Datetime_firstseen':'datetime64[ns]', 'Duration':float, 'Protocol':'category',
                'Src_IP_Addr':str, 'Src_Port':'category','Direc':str, 'Dst_IP_Addr':str, 'Dst_Port':'category',
                'Flags':str, 'Tos':'category', 'Packets':int, 'Bytes':int, 'Flows':int}
        for col in df.columns:
            df[col] = df[col].astype(col_types[col])
            
        df.to_feather(SOURCE+'/'+'feather_'+filename)

        fin = time.time()
        timer = int(fin-start)
        timer = [int(timer/60), int(timer%60)]
        print(f'Finished with {filename} -> {timer[0]}mins {timer[1]}secs')
        return df
    else:
        print('[ERROR] Incorrect file name: ', filename)
        
def convert_prefix_tonum(val:str) -> str:
    if val.endswith(' M'):
        val = val[:-2]
        val = int(float(val)*10**6)
        return str(val)
    elif val.endswith(' G'):
        val = val[:-2]
        val = int(float(val)*10**9)
        return str(val)
    else:
        return val
# %%
if __name__ == '__main__':
    conv_files = list(filter(lambda x: not x.startswith('feather_'), os.listdir(SOURCE)))
    print('[INFO] Starting conversion')
    print(f'[INFO] {len(conv_files)} files found in directory {os.getcwd()}{SOURCE[1:]}')
    pool = Pool(processes=4)
    dfs = pool.map(process_nfdump, conv_files)


