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

def getPostgre(aws, dbId):
     postgres = PostgresCollector(aws, dbId).start()

def getDataWithDuck(aws):
    return

# schedule.every(5).minutes.do(getAPI, aws)
# schedule.every().day.at("08:00").do(getPostgre, aws, 1)

# while True:
#     schedule.run_pending()
#     time.sleep(1)

getPostgre(aws,dbId = 1)
# getAPI(aws)

