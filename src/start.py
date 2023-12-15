import schedule
import time
import pandas as pd
from data_source.api import APICollector
from contracts.schema import BaseSchema
from tools.aws.client import S3Client

def test():
    aws = S3Client()
    # response = APICollector(BaseSchema, aws).start(50)
    return

schedule.every(5).minutes.do(test)

while True:
    schedule.run_pending()
    time.sleep(1)

test()