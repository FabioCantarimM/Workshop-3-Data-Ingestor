import schedule
import time
import pandas as pd
from data_source.api import APICollector
from contracts.schema import BaseSchema
from tools.aws.client import S3Client

def test():
    aws = S3Client()
    # response = APICollector(BaseSchema, aws).start(50)

    result = aws.athenaQuery("Select * from  'workshop'.'apicollectorapi'")
    # response = aws.download_file('api/api-response-2023-12-15T11:05:03.parquet')
    # parquet_data = response['Body'].read()
    # df = pd.read_parquet(parquet_data)

    # print(df)

    return
# schedule.every(5).minutes.do(test)

# while True:
#     schedule.run_pending()
#     time.sleep(1)

test()