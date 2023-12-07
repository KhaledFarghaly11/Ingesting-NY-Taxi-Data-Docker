#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from time import time
from sqlalchemy import create_engine

CHUNK_SIZE = 100000

def download_data(url):
    # Download the data
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'
    os.system(f"wget {url} -O {csv_name}")
    return csv_name

def process_and_insert_data(df, engine, table_name):
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    
    df.to_sql(name=table_name, con=engine, if_exists='append')

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    csv_name = download_data(url)

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=CHUNK_SIZE)

    # Create table and insert first chunk
    df = next(df_iter)
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    process_and_insert_data(df, engine, table_name)

    while True:
        try:
            t_start = time()
            df = next(df_iter)
            process_and_insert_data(df, engine, table_name)
            t_end = time()

            print('Inserted another chunk, took %.3f seconds' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the PostgreSQL database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to PostgreSQL')

    parser.add_argument('--user', required=True, help='user name for PostgreSQL')
    parser.add_argument('--password', required=True, help='password for PostgreSQL')
    parser.add_argument('--host', required=True, help='host for PostgreSQL')
    parser.add_argument('--port', required=True, help='port for PostgreSQL')
    parser.add_argument('--db', required=True, help='database name for PostgreSQL')
    parser.add_argument('--table_name', required=True, help='name of the table where results will be written')
    parser.add_argument('--url', required=True, help='URL of the CSV file')

    args = parser.parse_args()

    main(args)