from fastapi import FastAPI
from faker import Faker
import pandas as pd
import random

app = FastAPI()
fake = Faker()

# python3 -m uvicorn src.tools.fake-api.api:app --reload
nome_do_arquivo_csv = 'src/tools/fake-api/products.csv'
df = pd.read_csv(nome_do_arquivo_csv)
df['indice'] = range(1, len(df) + 1)
df.set_index('indice', inplace=True)


@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/gerar_compra")
async def gerar_compra():
    index = random.randint(1, len(df)-1)
    tuple = df.iloc[index]
    print(tuple["EAN"])
    return [{
            "client": fake.name(),
            "creditcard": fake.credit_card_provider(),
            "product": tuple["Product Name"],
            "ean": int(tuple["EAN"]),
            "price":  round(float(tuple["Price"])*1.2,2),
            "clientPosition": fake.location_on_land(),
            "store": 2,
            "dateTime": fake.iso8601()
        }]

@app.get("/gerar_compras/{numero_registros}")
async def gerar_compras(numero_registros: int):
    if numero_registros < 1:
        return {"error": "O número de registros deve ser pelo menos 1."}

    pessoas = []
    for _ in range(numero_registros):
        index = random.randint(1, len(df)-1)
        tuple = df.iloc[index]
        pessoa = {
            "client": fake.name(),
            "creditcard": fake.credit_card_provider(),
            "product": tuple["Product Name"],
            "ean": int(tuple["EAN"]),
            "price":  round(float(tuple["Price"])*1.2,2),
            "clientPosition": fake.location_on_land(),
            "store": 2,
            "dateTime": fake.iso8601()
        }
        pessoas.append(pessoa)

    return pessoas
