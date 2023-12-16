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
