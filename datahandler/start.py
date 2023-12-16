from tools.aws.client import S3Client
from contracts.schema import BaseSchemaPostgre
from tools.duckdb.duck import Duckdb

aws = S3Client()
myduck = Duckdb(aws)
tableName = 'transactionsPostgre'

# myduck.getData('api')
myduck.getData('postgre')

myduck.createTable(tableName, BaseSchemaPostgre )

# values = ['ean', 'price', 'store', 'dateTime']
values = ['id', 'transaction_id', 'transaction_time', 'ean', 'product_name',
       'price', 'store', 'pos_number', 'pos_system', 'pos_version',
       'pos_last_maintenance', 'operator', 'created_at', 'datasource']

# paths = ['datahandler/repository/api.parquet', 'datahandler/repository/postgre.parquet']
paths = ['datahandler/repository/postgre.parquet']

myduck.insertData(tableName,values,paths)

result = myduck.selectData(tableName)

myduck.exportToCSV(values,'datahandler/repository/result.csv', result)

myduck.closeCon()

print(result)