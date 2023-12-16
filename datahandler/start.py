from tools.aws.client import S3Client
from contracts.schema import BaseSchema
from tools.duckdb.duck import Duckdb

aws = S3Client()
myduck = Duckdb(aws)

myduck.getData('api')
# myduck.getData('postgre')

myduck.createTable('transactions', BaseSchema )

values = ['ean', 'price', 'store', 'dateTime']
# paths = ['datahandler/repository/api.parquet', 'datahandler/repository/postgre.parquet']
paths = ['datahandler/repository/api.parquet']

myduck.insertData('transactions',values,paths)

result = myduck.selectData('transactions')

print(result)