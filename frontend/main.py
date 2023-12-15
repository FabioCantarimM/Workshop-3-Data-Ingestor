import sys
from pathlib import Path

# Adiciona o diretório pai ao PYTHONPATH
sys.path.append(str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
from src.contracts.produtos import ProdutoSchema
from src.tools.aws.client import S3Client
from pydantic import ValidationError
import json
import io


# Funções da UI

def upload_file_ui():
    return st.file_uploader("Escolha o arquivo Excel", type="xlsx")


def display_errors(errors):
    for error in errors:
        st.error(error)


def confirm_upload():
    return st.button("Subir Dados")


# Função para processar o arquivo Excel


def process_excel(file):
    try:
        df = pd.read_excel(file, usecols="C:I", skiprows=10, nrows=210)
        valid_data = []
        errors = []

        for index, row in df.iterrows():
            try:
                produto = ProdutoSchema(**row.to_dict())
                valid_data.append(produto)
            except ValidationError as e:
                error_messages = []
                for error in e.errors():
                    field = error["loc"][0]
                    message = error["msg"]
                    error_messages.append(f"{field}: {message}")
                error_message = "; ".join(error_messages)
                errors.append(f"Linha {index + 11}: {error_message}")

        return valid_data, errors

    except Exception as e:
        return None, [f"Erro na leitura do arquivo: {str(e)}"]


# Função principal


def main():
    st.title("Aplicação de Upload de Produtos")

    uploaded_file = upload_file_ui()
    if uploaded_file is not None:
        data, errors = process_excel(uploaded_file)
        if errors:
            display_errors(errors)
        else:
            if confirm_upload():
                json_data = json.dumps([produto.model_dump_json() for produto in data])
                data_bytes = io.BytesIO(json_data.encode())
                S3Client().upload_file(data = data_bytes, s3_key="catalogo.parquet")
                st.success("Dados enviados com sucesso!")


if __name__ == "__main__":
    main()
