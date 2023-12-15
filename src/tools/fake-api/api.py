from fastapi import FastAPI
from faker import Faker

app = FastAPI()
fake = Faker()

# python3 -m uvicorn src.tools.fake-api.api:app --reload

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/gerar_compra")
async def gerar_compra():
    return [{
            "client": fake.name(),
            "creditcard": fake.credit_card_provider(),
            "ean": fake.ean(length=13),
            "price": fake.pricetag(),
            "clientPosition": fake.location_on_land(),
            "dateTime": fake.iso8601()
        }]

@app.get("/gerar_compras/{numero_registros}")
async def gerar_compras(numero_registros: int):
    if numero_registros < 1:
        return {"error": "O número de registros deve ser pelo menos 1."}

    pessoas = []
    for _ in range(numero_registros):
        pessoa = {
            "client": fake.name(),
            "creditcard": fake.credit_card_provider(),
            "ean": fake.ean(length=13),
            "price": fake.pricetag(),
            "clientPosition": fake.location_on_land(),
            "dateTime": fake.iso8601()
        }
        pessoas.append(pessoa)

    return pessoas
