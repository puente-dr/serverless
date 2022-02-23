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
# def export_to_s3_as_pickle_dataframe(s3_client, database, named_table: str):
#     """
#     Orchestrates export steps, encoding, and adds performance timer to logging
#     """
# 
#     export_time = time.time()
# 
#     # Serialize records as pickled Python Dictionary
#     table = database[named_table]
#     list_store: list = [
#         record
#         for record in table.find()
#     ]
# 
#     df = pd.DataFrame(list_store, columns=list(list_store[0].keys()))
#     print(tabulate(df.head(), headers=df.columns))
# 
#     # Write pickled Python Dictionaries to S3
#     file_name: str = f'store_pickle_dataframes/{to_snake_case(named_table)}.pickle'
# 
#     s3_client.put_object(
#         Bucket=Clients.S3_BUCKET_NAME,
#         Key=file_name,
#         Body=df.to_pickle()
#     )
# 
#     print(f'Writing to S3: \t\t S3://{Clients.S3_BUCKET_NAME}/{file_name}')
#     print(f'completed in \t\t {time.time() - export_time:.4f} seconds. \n')


if __name__ == '__main__':
    # load_json_from_s3(Clients.S3, 'form_specifications_v2')
    # load_pickle_dict_from_s3(Clients.S3, 'form_specifications_v2')
    load_pickle_list_from_s3(Clients.S3, 'form_specifications_v2')
