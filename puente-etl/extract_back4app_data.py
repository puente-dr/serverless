import os

import boto3
from dotenv import load_dotenv; load_dotenv()
from pymongo import MongoClient

from utils.db_schema import PuenteTables
from load_to_s3 import \
    export_to_s3_as_json, \
    export_to_s3_as_pickle_dataframe, \
    export_to_s3_as_pickle_dict


class BatchOutputs:
    JSON = 'json'
    PICKLE_DICT = 'pickle_dict'
    PICKLE_DATAFRAME = 'pickle_df'


def batch_export(serialization: str, named_puente_table=None):
    """
    Parameters
    ----------
    serialization: Class Variable, BatchOutputs, required
        Choose a data serialization method

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

    # Export all tables
    if named_puente_table is None:
        all_tables = remove_back4app_tables(
            db.list_collection_names()
        )
        for table_name in all_tables:
            export_table(serialization, s3_session, db, table_name)

    # Export one named table
    else:
        export_table(serialization, s3_session, db, named_puente_table)


def export_table(output_type: str, s3_client, database, table_name: str):
    if output_type == BatchOutputs.JSON:
        export_to_s3_as_json(s3_client, database, table_name)

    elif output_type == BatchOutputs.PICKLE_DATAFRAME:
        export_to_s3_as_pickle_dataframe(s3_client, database, table_name)

    elif output_type == BatchOutputs.PICKLE_DICT:
        export_to_s3_as_pickle_dict(s3_client, database, table_name)

    else:
        print('im a little teapot')


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
    batch_export(
        serialization=BatchOutputs.PICKLE_DATAFRAME,
        named_puente_table=PuenteTables.FORM_SPECIFICATIONS
    )
    # batch_export(PuenteTables.SURVEY_DATA)
    # batch_export()
