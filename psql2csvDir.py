import os, csv
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import Json
import time

############################## Config #################################### 
db = 'chembl'
user = 'pvgupta24'
pwd = ''
host = '127.0.0.1'
port = '5442'

samplesCount = 3000
dataDir = '/mnt/c/Users/t-pravgu/Documents/DataDirectory'
csvDir = f'{dataDir}/{db}3k/'


def psql2CsvDir():
    #Get list of tables
    sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type='BASE TABLE'"
    cursor.execute(sql)
    tables = cursor.fetchall()
    print(len(tables))
    for table in tables:
        print(table[0])
        query = f"SELECT * FROM {table[0]} LIMIT {samplesCount}"
        copyQuery = f'COPY ({query}) TO STDOUT WITH CSV HEADER'
        with open(f'{csvDir}{table[0]}.csv', 'w') as f:
            cursor.copy_expert(copyQuery, f)

def getSchemaRelations():
    sql = """SELECT 
    tc.table_schema, tc.constraint_name, tc.table_name, kcu.column_name, ccu.table_schema AS foreign_table_schema, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name 
    FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY';
    """
    cursor.execute(sql)
    relations = cursor.fetchall()
    print(len(relations))
    df = pd.DataFrame(columns=['column1', 'column2'])
    for row in relations:
        suff = '.csv'
        pairs = [f'{row[2]}{suff}/{row[3]}', f'{row[5]}{suff}/{row[6]}']
        pairs.sort()
        # break
        df = df.append({'column1': pairs[0], 'column2': pairs[1]}, ignore_index=True)
    df.to_csv(f'{dataDir}/{db}_gt.csv', index=False)


def getColList():
    df = pd.DataFrame(columns=['col', 'rowCount', 'columnDataType'])
    sql = "SELECT table_name, column_name, data_type from INFORMATION_SCHEMA.Columns where table_schema = 'public'"
    cursor.execute(sql)
    cols = cursor.fetchall()
    for row in cols:
        cursor.execute(f'SELECT count(*) FROM {row[0]}')
        rowCount = cursor.fetchone()[0]
        df = df.append({'col':f'{row[0]}.csv/{row[1]}', 'rowCount': rowCount, 'columnDataType': row[2]}, ignore_index=True)
    df.to_csv(f'{dataDir}/{db}.csv', index=False)


    
#Establishing the connection
conn = psycopg2.connect(
   database=db, user=user, password=pwd, host=host, port= port
)
cursor = conn.cursor()
psql2CsvDir()
# getColList()
# getSchemaRelations()

#Closing the connection
conn.close()