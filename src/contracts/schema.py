from typing import Union, Dict

# Definindo um tipo para o schema
GenericSchema = Dict[str, Union[str, float, int]]

# Definindo o schema base
BaseSchema: GenericSchema = {
    # Espera uma string
    "ean": str,

    # Espera um número (pode ser float ou int)
    "price": str,

    # Espera uma string, idealmente no formato de data
    "dateTime": str
}
