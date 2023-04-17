
import logging
import pandas as pd
import io
import requests
import time
from datetime import datetime


def main(name: str) -> str:
    logging.info(f"Python Activity trigger processing file {name} at {datetime.now()}")
    #time.sleep(30)
    url=name
    s=requests.get(url).content
    c=pd.read_csv(io.StringIO(s.decode('utf-8')))
    summary_result=c[['Base','Lat']].groupby('Base').count()
    logging.info(f"Summary for {name} is {summary_result['Lat'].to_json()}")
    return f"{summary_result['Lat'].to_json()}"