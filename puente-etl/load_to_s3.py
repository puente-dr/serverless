import os
import pickle
import time

import pandas as pd
from bson.json_util import dumps as mongo_dumps
from tabulate import tabulate
from dotenv import load_dotenv; load_dotenv()

from utils.helpers import to_snake_case


#
# JSON
#
def export_to_s3_as_json(s3_client, database, named_table):
    """
    Orchestrates export steps, encoding, and adds performance timer to logging
    """
    export_time = time.time()

    # Serialize records as JSON bytestream
    table = database[named_table]
    records: bytes = mongo_dumps(table.find()).encode('utf-8')

    # Write JSON to S3
    file_name: str = f'store_json/{to_snake_case(named_table)}.json'

    s3_client.put_object(
        Bucket=os.getenv('AWS_S3_BUCKET'),
        Key=file_name,
        Body=records
    )

    print(f'Writing to S3: \t\t S3://{os.getenv("AWS_S3_BUCKET")}/{file_name}')
    print(f'completed in \t\t {time.time() - export_time:.4f} seconds. \n')


#
# Pickle Python Dictionary
#
def export_to_s3_as_pickle_dict(s3_client, database, named_table: str):
    """
    Orchestrates export steps, encoding, and adds performance timer to logging
    """

    # mongo_dumps does not respect latin1 encoding, so we will pickle as dicts instead.
    # I turned myself into a pickle Morty! I'm Pickle Dict!

    export_time = time.time()

    # Serialize records as pickled Python Dictionary
    table = database[named_table]
    dict_to_pickle: dict = dict()
    for record in table.find():
        dict_to_pickle[record['_id']] = record
    records = pickle.dumps(dict_to_pickle)

    # Write pickled Python Dictionaries to S3
    file_name: str = f'store_pickle_dicts/{to_snake_case(named_table)}.pickle'

    s3_client.put_object(
        Bucket=os.getenv('AWS_S3_BUCKET'),
        Key=file_name,
        Body=records
    )

    print(f'Writing to S3: \t\t S3://{os.getenv("AWS_S3_BUCKET")}/{file_name}')
    print(f'completed in \t\t {time.time() - export_time:.4f} seconds. \n')


#
# Pickle Pandas DataFrame
#
def export_to_s3_as_pickle_dataframe(s3_client, database, named_table: str):
    """
    Orchestrates export steps, encoding, and adds performance timer to logging
    """

    export_time = time.time()

    # Serialize records as pickled Python Dictionary
    table = database[named_table]
    list_store: list = [
        record
        for record in table.find()
    ]

    df = pd.DataFrame(list_store, columns=list(list_store[0].keys()))
    print(tabulate(df.head(), headers=df.columns))

    # Write pickled Python Dictionaries to S3
    file_name: str = f'store_pickle_dataframes/{to_snake_case(named_table)}.pickle'

    s3_client.put_object(
        Bucket=os.getenv('AWS_S3_BUCKET'),
        Key=file_name,
        Body=df.to_pickle()
    )

    print(f'Writing to S3: \t\t S3://{os.getenv("AWS_S3_BUCKET")}/{file_name}')
    print(f'completed in \t\t {time.time() - export_time:.4f} seconds. \n')
