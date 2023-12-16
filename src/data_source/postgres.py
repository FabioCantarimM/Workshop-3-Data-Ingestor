import pandas as pd
from tools.db.database_connection import Session
from contracts.transactions import Transaction
import os
from datetime import datetime
from io import BytesIO
from tools.aws.client import S3Client
from contracts.transactions import Transaction

class PostgresCollector:
    def __init__(self, aws_client: S3Client):
        self._model = Transaction
        self._buffer = None
        self._fileName = os.environ.get("DB_NAME")
        self._aws = aws_client
        

    def start(self):
        df = self.extract_data()
        print("Processo extract com sucesso")
        df = self.transform_add_columns(df, "postgres")
        print("Processo extract com sucesso")
        self.convert_to_delta(df)

        if self._buffer is not None:
            file_name = self.fileName()
            print(file_name)
            self._aws.upload_file(self._buffer, file_name)
            return True
        
        return False

    def extract_data(self):
        session = Session()
        df = pd.read_sql(session.query(self._model).statement, session.bind)
        session.close()
        return df
    
    def transform_add_columns(self, df, datasource_value):
        df['created_at'] = datetime.now()
        df['datasource'] = datasource_value
        return df
    
    def convert_to_delta(self, df):
        try:
            # Converte o DataFrame do pandas para uma tabela PyArrow
            self._buffer = BytesIO()
            try:
                 df.to_parquet(self._buffer)
                 return self._buffer
            except:
                print("Error ao transformar o Df em Parquet")
                self._buffer = None

        except Exception as e:
            print(f"Erro ao converter DataFrame para Delta: {e}")
            self._buffer = None
    

    def fileName(self):
            data_atual = datetime.datetime.now().isoformat()
            match = data_atual.split(".")
            return f'postgres/{self._fileName}-{match[0]}.parquet'