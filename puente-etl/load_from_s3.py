import json
import io
import pickle

import pandas as pd

from utils.clients import Clients


#
# Load JSON
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
# Load Pandas DataFrame
#
def load_dataframe_from_s3(s3_client, file_name: str):
    print('load_pickle_dict_from_s3')
    response = s3_client.get_object(
        Bucket=Clients.S3_BUCKET_NAME,
        Key=f'store_dataframes/{file_name}.df',
    )

    pickled_df_buffer = io.BytesIO(response['Body'].read())
    df = pd.read_pickle(pickled_df_buffer)

    return df


#
# Load Python Dictionary
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
# Load Python List
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


if __name__ == '__main__':
    # load_json_from_s3(Clients.S3, 'form_specifications_v2')
    # load_pickle_dict_from_s3(Clients.S3, 'form_specifications_v2')
    load_pickle_list_from_s3(Clients.S3, 'form_specifications_v2')
