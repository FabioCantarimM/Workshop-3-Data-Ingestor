import os
from dotenv import load_dotenv
import duckdb
from deltalake import DeltaTable

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Acessa as variáveis de ambiente
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
delta_table_path = os.environ.get("DELTA_LAKE_S3_PATH")

# Ler a tabela Delta usando delta-rs
dt = DeltaTable(delta_table_path)
pyarrow_dataset = dt.to_pyarrow_dataset()

# Conectar com DuckDB e criar um objeto de consulta
con = duckdb.connect()
sample_dataset = con.from_arrow(pyarrow_dataset)

# Executar uma consulta de exemplo e fechar a conexão DuckDB
query = "SELECT * FROM transacion"
result = sample_dataset.execute(query).fetchall()
# print(result)
con.close()
