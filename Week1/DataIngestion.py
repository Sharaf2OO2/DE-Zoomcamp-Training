import pandas as pd, argparse, os
from sqlalchemy import create_engine
from time import time

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name = 'output.parquet'

    os.system(f'wget {url} -O {parquet_name}')

    df = pd.read_parquet(parquet_name)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df.head(0).to_sql(name=table_name,con=engine,if_exists='replace')

    chunksize = 100000
    num = 1

    for i in range(0,len(df),chunksize):
        timestart = time()

        df_chunked = df[i:i + chunksize]
        
        df_chunked.to_sql(name=table_name,con=engine,if_exists='append')
        
        timeend = time()
        
        print(f'Chunk {num} inserted, took {timeend - timestart:.3f} seconds')

        num += 1

if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where the result will be written to')
    parser.add_argument('--url', help='url of the parquet file')

    args = parser.parse_args()

    main(args)