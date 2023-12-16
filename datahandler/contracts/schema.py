from typing import Union, Dict

# Definindo um tipo para o schema
GenericSchema = Dict[str, Union[str, float, int]]

# Definindo o schema base
BaseSchema: GenericSchema = {
    "ean" : "VARCHAR",
    "price" : "FLOAT",
    "store" : "INT",
    "dateTime" : "TIMESTAMP"
}

BaseSchemaPostgre: GenericSchema = {
    'id': "INT",
    'transaction_id': "VARCHAR",
    'transaction_time': "TIMESTAMP",
    'ean': "VARCHAR",
    'product_name': "VARCHAR",
    'price': "FLOAT",
    'store': "INT",
    'pos_number': "INT",
    'pos_system': "VARCHAR",
    'pos_version': "INT",
    'pos_last_maintenance': "VARCHAR",
    'operator' : "INT",
    'created_at ': 'TIMESTAMP',
    'datasource' : 'VARCHAR'
}
