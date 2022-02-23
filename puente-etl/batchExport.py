import os
import pickle
import sys; sys.path.append(os.path.join(os.path.dirname(__file__)))
import time

import boto3
import pandas as pd
from bson.json_util import dumps as mongo_dumps
from dotenv import load_dotenv; load_dotenv()
from pymongo import MongoClient
from tabulate import tabulate

from db_schema import PuenteTables
from utils import to_snake_case


def batch_export(named_puente_table=None):
    """
    Parameters
    ----------
    named_puente_table: Class Variable, PuenteTables class, optional
        Back4App Table Name

    Returns
    ------
    Logging

    """

    # Initialize AWS S3 Client
    s3_session = boto3.client(
        "s3",
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    )

    # Initialize MongoDB Client
    client = MongoClient(os.getenv('DATABASE_URI'))

    #
    # Make Connection to Database
    #
    # The name that Back4App gives its databases can be found
    # after the last slash in MongoDB URI string
    db_name = client.get_default_database().name
    db = client[db_name]

    # Export one named table
    if named_puente_table is not None:
        export_database_table(s3_session, db, named_puente_table)

    # Export all tables
    else:
        all_tables = remove_back4app_tables(
            db.list_collection_names()
        )
        for table_name in all_tables:
            export_database_table(s3_session, db, table_name)


def export_database_table(s3_client, database, named_table):
    """
    Orchestrates export steps, encoding, and adds performance timer to logging
    """

    export_time = time.time()

    table = database[named_table]

    # records = serialize_records_as_json_bytestream(table)
    # records = serialize_records_as_python_dict(table)
    records = serialize_records_as_pandas_dataframe(table)

    # write_json_to_s3(s3_client, records, named_table)
    # write_pickles_to_s3(s3_client, records, named_table)

    print(f'completed in \t\t {time.time() - export_time:.4f} seconds. \n')


def serialize_records_as_json_bytestream(mongodb_contents):
    return mongo_dumps(
        mongodb_contents.find()
    ).encode('utf-8')


def write_json_to_s3(s3_client, data: bytes, table_name: str):

    file_name = f'store_json/{to_snake_case(table_name)}.json'

    s3_client.put_object(
        Bucket=os.getenv('AWS_S3_BUCKET'),
        Key=file_name,
        Body=data
    )

    print(f'Writing to S3: \t\t S3://{os.getenv("AWS_S3_BUCKET")}/{file_name}')


def serialize_records_as_python_dict(mongodb_contents):
    # mongo_dumps does not respect latin1 encoding, so we will pickle as dicts instead.
    # I turned myself into a pickle Morty! I'm Pickle Dict!

    dict_store = dict()
    for record in mongodb_contents.find():
        dict_store[record['_id']] = record

    return pickle.dumps(dict_store)


def write_pickles_to_s3(s3_client, data: bytes, table_name: str):

    file_name = f'store_pickle_dicts/{to_snake_case(table_name)}.pickle'

    s3_client.put_object(
        Bucket=os.getenv('AWS_S3_BUCKET'),
        Key=file_name,
        Body=data
    )

    print(f'Writing to S3: \t\t S3://{os.getenv("AWS_S3_BUCKET")}/{file_name}')


def serialize_records_as_pandas_dataframe(mongodb_contents):
    dict_store = dict()
    for record in mongodb_contents.find():
        dict_store[record['_id']] = record

    df = pd.json_normalize(dict_store)

    print('Denormalized Custom Form: ')
    print(tabulate(df.head(), headers=df.columns))

    return df


def remove_back4app_tables(all_tables: list) -> list:
    """
    Exclude the Back4App Metadata tables
    """

    tables_to_keep = []
    for t in all_tables:
        if t[0] == '_':
            if t.strip('_') in ('Role', 'Session', 'User'):
                tables_to_keep.append(t)
        else:
            if not any(i in t for i in ['B4a', 'b4a', 'system.profile']):
                tables_to_keep.append(t)

    return tables_to_keep


if __name__ == '__main__':
    batch_export(PuenteTables.FORM_SPECIFICATIONS)
    # batch_export(PuenteTables.SURVEY_DATA)
    # batch_export()