import os, csv, time
from collections import Counter
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import Json
from collections import Counter

############################## Config #################################### 

# csvDir = f'/mnt/c/Users/t-pravgu/Documents/DataDirectory/Cosmos-Streams/'

db = 'pvgupta24'
user = 'pvgupta24'
pwd = ''
host = '127.0.0.1'
port = '5442'

dataset = 'cosmos'
algorithms = ['merge_probe_cost_model_greedy', 'merge_distinct_list', 'lsh_ensemble_precision_90']
minOverlap = 2

# inverted_indices_table = f'{dataset}_inverted_lists'
sets_table = f'{dataset}_sets'
#Establishing the connection
conn = psycopg2.connect(
   database=db, user=user, password=pwd, host=host, port= port
)
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

for algorithm in algorithms:
    root_path = f'results/cosmos/100/{algorithm}/'
    results_csv = f'{root_path}100_5.csv'
    output_csv = f'{root_path}processed_pairs_100_5.csv'
    df = pd.read_csv(results_csv).fillna(value = '')
    df2 = pd.DataFrame(columns= ['column1', 'column2', 'overlap'])

    datetime = ['date', 'time']
    numeric = ['num', 'total', 'avg', 'percentage']
    numeric_count, datetime_count = 0, 0

    start_time = time.time()
    for id, row in df.iterrows():
        query_id = row['query_id']
        cursor.execute(f'SELECT raw_set_name FROM {sets_table} WHERE id={query_id}')
        set1_name = cursor.fetchone()[0].tobytes().decode()
        for match in filter(None, row['results'].split('s')):
            arr = match.split('o')
            match_id = arr[0]
            match_overlap = int(arr[1])
            if match_overlap < minOverlap:
                continue
            cursor.execute(f'SELECT raw_set_name FROM {sets_table} WHERE id={match_id}')
            set2_name = cursor.fetchone()[0].tobytes().decode()
            stream_col1 = set1_name.split('/')
            stream_col2 = set2_name.split('/')

            # Skips cols of same stream
            if stream_col1[0] == stream_col2[0]:
                continue

            if any(w in stream_col1[1].lower() or w in stream_col2[1].lower() for w in datetime):
                datetime_count+=1
            elif any(w in stream_col1[1].lower() or w in stream_col2[1].lower() for w in numeric):
                numeric_count+=1

            elif set1_name < set2_name:
                df2 = df2.append({'column1': set1_name, 'column2': set2_name, 'overlap': match_overlap }, ignore_index=True)        
            elif set1_name > set2_name:
                df2 = df2.append({'column1': set2_name, 'column2': set1_name, 'overlap': match_overlap }, ignore_index=True)
    end_time = time.time()
    print(f'Time: {str(end_time - start_time)}')
    print(f'Total date/time cols: {datetime_count}')
    print(f'Total numeric cols: {numeric_count}')
    
    df2.drop_duplicates(['column1', 'column2'], keep="first", inplace=True)
    df2 = df2.sort_values(by='overlap', ascending=False)
    df2.to_csv(output_csv, index=False)
    print(f'Done. Output: {output_csv}')
    print('Total rows:', df2.shape[0])
    print(Counter(df2['overlap']).most_common(5))

#Closing the connection
conn.close()
