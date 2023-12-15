import pandas as pd
from tools.db.database_connection import Session
from contracts.transactions import Transaction
from datetime import datetime
from deltalake.writer import write_deltalake
import pyarrow as pa
from tools.aws.client import S3Client

class PostgresCollector:
    def __init__(self, model: Transaction, aws_client: S3Client):
        self._model = model
        self._aws_access_key_id = aws_client._envs["aws_access_key_id"]
        self._aws_secret_access_key = aws_client._envs["aws_secret_access_key"]
        self._region_name = aws_client._envs["region_name"]
        self._s3_bucket = aws_client._envs["s3_bucket"]
        self._datalake = aws_client._envs["datalake"]

    def start(self):
        df = self.extract_data()
        print("Processo extract com sucesso")
        df = self.transform_add_columns(df, "postgres")
        print("Processo extract com sucesso")
        self.convert_to_delta(df)
        # O código relacionado ao upload para o S3 foi removido
        print("Processo finalizado com sucesso")
        return True

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
            arrow_table = pa.Table.from_pandas(df)

            # Define o URI da tabela Delta e as opções de armazenamento
            print(f"S3 Bucket: {self._s3_bucket}")
            print(f"Datalake Path: {self._datalake}")
            delta_table_uri = f"{self._datalake}"
            print(f"Delta Table URI: {delta_table_uri}")
            storage_options = {
                "AWS_ACCESS_KEY_ID": self._aws_access_key_id,
                "AWS_SECRET_ACCESS_KEY": self._aws_secret_access_key,
                "region": self._region_name,
            }
            # Salva a tabela PyArrow como uma tabela Delta
            write_deltalake(
                data=arrow_table,
                table_or_uri=delta_table_uri,
                mode="overwrite",
                overwrite_schema=True,
                storage_options=storage_options,
            )
        except Exception as e:
            print(f"Erro ao converter DataFrame para Delta: {e}")
            return False
        return True
    
