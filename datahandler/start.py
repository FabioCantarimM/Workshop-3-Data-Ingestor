from tools.aws.client import S3Client
from contracts.schema import BaseSchema
from tools.duckdb.duck import Duckdb

aws = S3Client()
myduck = Duckdb(aws)

myduck.getData('catalogo')
# myduck.getData('postgre')

myduck.createTable('transactions', BaseSchema )

values = ['ean', 'price', 'store', 'dateTime']
# paths = ['datahandler/repository/api.parquet', 'datahandler/repository/postgre.parquet']
paths = ['datahandler/repository/api.parquet']

myduck.insertData('transactions',values,paths)

result = myduck.selectData('transactions')

myduck.exportToCSV(['ean', 'price', 'store', 'dateTime'],'datahandler/repository/result.csv', result)

print(result)