from tools.aws.client import S3Client
import pyarrow.parquet as pq
import pandas as pd
from io import BytesIO
import os
import duckdb

def getData(path, local_folder):
    aws = S3Client()

    files = aws.list_object(path)

    print(files)
    dfs = []

    for file in files:
        download_path = file['Key']
        result = aws.download_file(download_path)
        df_parquet = pd.read_parquet(BytesIO(result['Body'].read()))
        dfs.append(df_parquet)

    df = pd.concat(dfs, ignore_index=True)
    
    #salvando na memoria local

    os.makedirs(local_folder, exist_ok=True)
    local_path = f"{local_folder}/{path}.parquet"
    df.to_parquet(local_path, index=False)


    #conectar no duckdb
    con_duckdb = duckdb.connect(database=':memory:', read_only=False)

    df_parquet = pd.read_parquet(f'./{local_path}')

    tabela_duckdb = 'test'
    
    create_table_query = f"""
        CREATE TABLE {tabela_duckdb} (
            ean VARCHAR,
            price FLOAT,
            store INT,
            dateTime TIMESTAMP
        )
        """

    con_duckdb.execute(create_table_query)

    insert_data_query = f"INSERT INTO {tabela_duckdb} VALUES (?, ?, ? ,?)"

    con_duckdb.executemany(insert_data_query, df.values.tolist())

    result = con_duckdb.execute(f"SELECT price FROM {tabela_duckdb} where price > 30").fetchall()

    print(result)

    con_duckdb.close()

    return

getData('api', 'repository')