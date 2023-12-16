from contracts.schema import GenericSchema
from tools.aws.client import S3Client
import pandas as pd
from pandas import DataFrame
from io import BytesIO
import os
import duckdb
import csv

class Duckdb:

    def __init__(self, aws_client:S3Client):
        self._aws = aws_client
        self._local_folder = 'datahandler/repository'
        self._con =  duckdb.connect(database=':memory:', read_only=False)
        return

    def getData(self, path):
        files =  self._aws.list_object(path)
        dfs = []

        for file in files:
            download_path = file['Key']
            result = self._aws.download_file(download_path)
            df_parquet = pd.read_parquet(BytesIO(result['Body'].read()))
            dfs.append(df_parquet)

        df = pd.concat(dfs, ignore_index=True)
        self.saveData(df, path)
        return True

    def saveData(self, df: DataFrame, path: str):
        os.makedirs(self._local_folder, exist_ok=True)
        local_path = f"{self._local_folder}/{path}.parquet"
        df.to_parquet(local_path, index=False)

    def createTable(self, tableName: str, tableSchema:GenericSchema):
        columns = [f"{column} {type}" for column, type in tableSchema.items()]
        query = f"CREATE TABLE {tableName} ({', '.join(columns)})"
        self._con .execute(query)


    def closeCon(self):
        self._con.close()


    def insertData(self, tableName: str, values: list, files: list):
        placeholders = ', '.join(['?' for _ in values])
        query = f"INSERT INTO {tableName} VALUES ({placeholders})"

        for file_path in files:
            df_parquet = pd.read_parquet(f'./{file_path}')
            self._con.executemany(query, df_parquet.values.tolist())    

    def selectData(self, tableName: str, values: list = None, where: str = None, group: str = None, order: str = None ):
        if values is None:
            values = "*"
        else:
            values = '"'+'", "'.join(values)
        where_clause = f'WHERE {where}' if where else ''
        group_clause = f'GROUP BY {group}' if group else ''
        order_clause = f'ORDER BY {order}' if order else ''

        # Set values to an empty string if it was None
        values = values or ''

        query = f'SELECT {values} FROM {tableName} {where_clause} {group_clause} {order_clause}'
        return self._con.execute(query).fetchall()
    
    def exportToCSV(self, header,path:str, data):
        content: list = []
        with open(path, 'w', newline='') as file_csv:
           writer_csv = csv.writer(file_csv)
           writer_csv.writerow(header)
           writer_csv.writerows(data)

    def dropTable(self, tableName):
        query = f'Drop table {tableName}'
        self._con.execute(query)