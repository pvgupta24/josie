import os, csv
from collections import Counter
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import Json
import time

############################## Config #################################### 

# csvDir = f'/mnt/c/Users/t-pravgu/Documents/DataDirectory/Cosmos-Streams/'
csvDir = f'/mnt/c/Users/t-pravgu/Documents/DataDirectory/Cosmos/'

db = 'pvgupta24'
user = 'pvgupta24'
pwd = ''
host = '127.0.0.1'
port = '5442'

dataset = 'cosmos'

inverted_indices_table = f'{dataset}_inverted_lists'
sets_table = f'{dataset}_sets'


# From https://github.com/ekzhu/SetSimilaritySearch/blob/master/SetSimilaritySearch/utils.py
def _frequency_order_transform(sets):
    """Transform tokens to integers according to global frequency order.
    This step replaces all original tokens in the sets with integers, and
    helps to speed up subsequent prefix filtering and similarity computation.
    See Section 4.3.2 in the paper "A Primitive Operator for Similarity Joins
    in Data Cleaning" by Chaudhuri et al..
    Args:
        sets (dict): a list of sets, each entry is an iterable representing a
            set.
    Returns:
        sets (dict): a list of sets, each entry is a sorted Numpy array with
            integer tokens replacing the tokens in the original set.
        order (dict): a dictionary that maps token to its integer representation
            in the frequency order.
    """
    # logging.debug("Applying frequency order transform on tokens...")
    counts = Counter(token for s in list(sets.values()) for token in s).most_common()
    order = dict()
    inverted_list = [{}] * len(counts) #dict()
    print(f'Total tokens = {len(counts)}')
    counts = reversed(counts)
    for i, (token, freq) in enumerate(counts):
        order[token] = i
        inverted_list[i] = {}
        inverted_list[i]['token'] = i
        inverted_list[i]['frequency'] = freq
        inverted_list[i]['raw_token'] = bytearray(str(token), 'utf-8')
        inverted_list[i]['set_ids'] = []

    new_sets = [{}]*len(sets.items()) #dict()
    for i, (key, curset) in enumerate(sets.items()):
        new_sets[i] = {}
        new_sets[i]['raw_set_name'] = str(key)
        new_sets[i]['id'] = i
        new_sets[i]['tokens'] = [order[token] for token in curset]
        # new_sets[i]['raw_tokens'] = curset
        new_sets[i]['tokens'].sort()
        new_sets[i]['size'] = len(new_sets[i]['tokens'])
        for pos, tokenId in enumerate(new_sets[i]['tokens']):
            # set_id, set_size, matching_pos for token
            inverted_list[tokenId]['set_ids'].append((i, new_sets[i]['size'], pos))
        new_sets[i]['num_non_singular_token'] = sum(1 for tokenId in new_sets[i]['tokens'] if inverted_list[tokenId]['frequency'] > 1)

    tokenId = 0
    inverted_list[tokenId]['duplicate_group_id'] = 0
    inverted_list[tokenId]['set_ids'].sort()
    temp_unzip = [list(t) for t in zip(*inverted_list[tokenId]['set_ids'])]
    inverted_list[tokenId]['match_positions'] = temp_unzip[2]
    inverted_list[tokenId]['set_sizes'] = temp_unzip[1]
    inverted_list[tokenId]['set_ids'] = temp_unzip[0]
    groupCounts = []
    groupCount = 1
    curGroup = 0
    # print(inverted_list[0])
    
    for i in range(1, len(inverted_list)):
        try:
            # print(inverted_list[i])                
            inverted_list[i]['duplicate_group_id'] = 0
            inverted_list[i]['set_ids'].sort()
            temp_unzip = [list(t) for t in zip(*(inverted_list[i]['set_ids']))]
            inverted_list[i]['match_positions'] = temp_unzip[2]
            inverted_list[i]['set_sizes'] = temp_unzip[1]
            inverted_list[i]['set_ids'] = temp_unzip[0]
        except Exception as e: 
            print(e)
            print(i)
            # print(len(inverted_list[i]['set_ids']))
            # print(inverted_list[i]['set_ids'][2])

        if inverted_list[i]['set_ids'] == inverted_list[i-1]['set_ids']:
            groupCount += 1
        else:
            curGroup += 1
            groupCounts.append(groupCount)
            groupCount = 1
        inverted_list[i]['duplicate_group_id'] = curGroup
    groupCounts.append(groupCount)

    for i in range(len(inverted_list)):
        inverted_list[i]['duplicate_group_count'] = groupCounts[inverted_list[i]['duplicate_group_id']]
    
    print("Done applying frequency order.")
    return new_sets, inverted_list

def _load_sets(csv_dir):
    sets = dict()
    # i = 0
    for f in os.listdir(csv_dir):
        # if i > 3:
        #     break
        # i += 1
        path = os.path.join(csv_dir, f)
        # print(path)
        try:
            df = pd.read_csv(path).fillna('') 
            for col in list(df):
                # col_name = col.replace('"','').replace("'", "")
                sets[f'{f}/{col}'] = set(filter(None, df[col].tolist()))  
        except Exception as e: 
            print(e, f)
    print(f'Total Sets read = {len(sets)}')
    return sets

start_time = time.time()
sets = _load_sets(csvDir)
load_time = time.time()
print(f'Load Time: {str(load_time - start_time)}')
exit(0)
new_sets, inverted_list = _frequency_order_transform(sets)
invert_time = time.time()
print(f'Global Ordering, Inverted Indices Time: {str(invert_time - load_time)}')

# print(new_sets[0])
# print(inverted_list[0])

#Establishing the connection
conn = psycopg2.connect(
   database=db, user=user, password=pwd, host=host, port= port
)
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#Droping  table if already exists.
cursor.execute(f'DROP TABLE IF EXISTS {inverted_indices_table}')
#Creating table as per requirement
sql = f'''CREATE TABLE {inverted_indices_table}(
            token integer primary key,
            frequency integer not null,
            duplicate_group_id integer not null,
            duplicate_group_count integer not null,
            set_ids integer[] not null,
            set_sizes integer[] not null,
            match_positions integer[] not null,
            raw_token bytea
        )'''
cursor.execute(sql)
print(f"{inverted_indices_table} Table created successfully........")

cursor.execute(f'DROP TABLE IF EXISTS {sets_table}')
sql = f'''CREATE TABLE {sets_table}(
            id integer primary key,
            raw_set_name bytea,
            size integer not null,
            num_non_singular_token integer not null,
            tokens integer[] not null
        )'''
cursor.execute(sql)
print(f"{sets_table} Table created successfully........")
conn.commit()

def sql_insert_statement(tableName, data_dict):
    # columns = data_dict.keys()
    # values = [data_dict[column] for column in columns]
    # insert_statement = 'insert into song_table (%s) values %s'
    # return cursor.mogrify(insert_statement, (AsIs(','.join(columns)), tuple(values)))
    sql = '''
        INSERT INTO %s(%s) VALUES (%%(%s)s );
        '''   % (tableName, ',  '.join(data_dict),  ')s, %('.join(data_dict))
    return sql

# print(cursor.mogrify(sql_insert_statement(sets_table, new_sets[0])))

cursor.executemany(sql_insert_statement(sets_table, new_sets[0]), new_sets)
print(f"Added rows to {sets_table}")

cursor.executemany(sql_insert_statement(inverted_indices_table, inverted_list[0]), inverted_list)
print(f"Added rows to {inverted_indices_table}")
conn.commit()
#Closing the connection
conn.close()


insertion_time = time.time()
print(f'DB Insertion Time: {str(insertion_time - invert_time)}')