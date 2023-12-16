import schedule
import time
import pandas as pd
from data_source.api import APICollector
from contracts.schema import BaseSchema
from tools.aws.client import S3Client

from data_source.postgres import PostgresCollector

aws = S3Client()

def getAPI(aws):
    response = APICollector(BaseSchema, aws).start(50)

def getPostgre(aws):
     postgres = PostgresCollector(aws).start()

# schedule.every(5).minutes.do(getAPI, aws)
# schedule.every().day.at("08:00").do(getPostgre, aws)

# while True:
#     schedule.run_pending()
#     time.sleep(1)

getPostgre(aws)
# getAPI(aws)

