# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json
import pandas as pd

import azure.functions as func
import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    files = [
        "https://raw.githubusercontent.com/fivethirtyeight/uber-tlc-foil-response/master/uber-trip-data/uber-raw-data-apr14.csv",
        "https://raw.githubusercontent.com/fivethirtyeight/uber-tlc-foil-response/master/uber-trip-data/uber-raw-data-may14.csv",
        "https://raw.githubusercontent.com/fivethirtyeight/uber-tlc-foil-response/master/uber-trip-data/uber-raw-data-jun14.csv",
        "https://raw.githubusercontent.com/fivethirtyeight/uber-tlc-foil-response/master/uber-trip-data/uber-raw-data-jul14.csv",
        "https://raw.githubusercontent.com/fivethirtyeight/uber-tlc-foil-response/master/uber-trip-data/uber-raw-data-aug14.csv",
        "https://raw.githubusercontent.com/fivethirtyeight/uber-tlc-foil-response/master/uber-trip-data/uber-raw-data-sep14.csv"
    ]
    tasks = []
    for file in files:
        tasks.append(context.call_activity("FetchData", file))
    
    file_summary_results = yield context.task_all(tasks)
    file_results = file_summary_results
    #logging.info(f"Summary of file={file_results}")
    data_dict = [json.loads(x) for x in file_results]
    df = pd.DataFrame.from_dict(data_dict, orient='columns')
    logging.info(f"Summary of file")
    logging.info(f"{df}")
    final_result = {}

    for d in data_dict:
        for k, v in d.items():
            if k in final_result:
                final_result[k] += v
            else:
                final_result[k] = v

    logging.info(f"result={final_result}")
    return final_result

main = df.Orchestrator.create(orchestrator_function)