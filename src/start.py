import schedule
import time
import pandas as pd
from data_source.api import APICollector
from contracts.schema import BaseSchema
from tools.aws.client import S3Client

from data_source.postgres import PostgresCollector
from contracts.transactions import Transaction

# def test():
#     aws = S3Client()
#     # response = APICollector(BaseSchema, aws).start(50)
#     return

# schedule.every(5).minutes.do(test)
aws = S3Client()
postgres = PostgresCollector(Transaction, aws)
postgres.start()

