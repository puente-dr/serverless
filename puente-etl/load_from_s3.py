import json
import pickle
import pprint

import pandas as pd
from tabulate import tabulate

from utils.clients import Clients


#
# JSON
#
def load_json_from_s3(s3_client, file_name: str):
    print('load_json_from_s3')
    response = s3_client.get_object(
        Bucket=Clients.S3_BUCKET_NAME,
        Key=f'store_json/{file_name}.json',
    )
    return json.loads(
        response['Body'].read().decode('utf-8')
    )


#
# Pickle Python Dictionary
#
def load_pickle_dict_from_s3(s3_client, file_name: str):
    print('load_pickle_dict_from_s3')
    response = s3_client.get_object(
        Bucket=Clients.S3_BUCKET_NAME,
        Key=f'store_pickle_dicts/{file_name}.pickle',
    )
    raw = response['Body'].read()
    data = pickle.loads(raw)
    return data


#
# Pickle Python List
#
def load_pickle_list_from_s3(s3_client, file_name: str):
    print('load_pickle_list_from_s3')
    response = s3_client.get_object(
        Bucket=Clients.S3_BUCKET_NAME,
        Key=f'store_pickle_lists/{file_name}.pickle',
    )
    raw = response['Body'].read()
    data = pickle.loads(raw)
    return data


#
# Pickle Pandas DataFrame
#
def load_pickle_dataframe_from_s3(s3_client, file_name: str):
    response = s3_client.get_object(
        Bucket=Clients.S3_BUCKET_NAME,
        Key=file_name
    )
    raw = response['Body'].read()
    data = pickle.loads(raw)

    df = pd.DataFrame(data, columns=list(data[0].keys()))
    print(tabulate(df.head(), headers=df.columns))


if __name__ == '__main__':
    # load_json_from_s3(Clients.S3, 'form_specifications_v2')
    # load_pickle_dict_from_s3(Clients.S3, 'form_specifications_v2')
    load_pickle_list_from_s3(Clients.S3, 'form_specifications_v2')
