import os, csv
from collections import Counter
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import Json

############################## Config #################################### 

# csvDir = f'/mnt/c/Users/t-pravgu/Documents/DataDirectory/Cosmos-Streams/'

db = 'pvgupta24'
user = 'pvgupta24'
pwd = ''
host = '127.0.0.1'
port = '5442'

dataset = 'cosmos'
inverted_indices_table = f'{dataset}_inverted_lists'
sets_table = f'{dataset}_sets'
f1 = bytearray('Cafe_Defualt_Structred_Stream.csv/srcDC', 'utf-8')
f2 = bytearray('IPFIX_InterDC_FiveMin.csv/DstDC', 'utf-8')
#Establishing the connection
conn = psycopg2.connect(
   database=db, user=user, password=pwd, host=host, port= port
)
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
cursor.execute(f'SELECT tokens FROM {sets_table} WHERE raw_set_name=%s', (f1,))
tokens1 = cursor.fetchone()[0]
    
cursor.execute(f'SELECT tokens FROM {sets_table} WHERE raw_set_name=%s', (f2,))
tokens2 = cursor.fetchone()[0]
common = list(filter(lambda x: x in tokens1, tokens2))
print(f'len1 common: {len(tokens1)}')
print(f'len2 common: {len(tokens2)}')

print(f'Length common: {len(common)}')

for id in common:
    cursor.execute(f'SELECT raw_token FROM {inverted_indices_table} WHERE token={id}')
    token = cursor.fetchone()[0].tobytes().decode()
    print(token)
#Closing the connection
conn.close()
