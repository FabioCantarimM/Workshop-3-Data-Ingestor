from typing import Union, Dict

# Definindo um tipo para o schema
GenericSchema = Dict[str, Union[str, float, int]]

# Definindo o schema base
BaseSchema: GenericSchema = {
    # Espera uma string
    "ean": int,

    # Espera um número (Float)
    "price": float,

    # Espera um número (Inteiro)
    "store": int,

    # Espera uma string, idealmente no formato de data
    "dateTime": str
}
