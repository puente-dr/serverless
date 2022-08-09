import io
import pickle
import time

from bson.json_util import dumps as mongo_dumps
from pandas import DataFrame

from utils.clients import Clients
from utils.constants import PuenteTables
from utils.helpers import to_snake_case


#
# JSON
#
def export_to_s3_as_json(s3_client, mongo_client, named_table: str):
    """
    Orchestrates export steps, encoding, and adds performance timer to logging
    """
    export_time = time.time()

    # Serialize records as JSON bytestream
    table = mongo_client[named_table]
    records: bytes = mongo_dumps(table.find()).encode('utf-8')

    # Write JSON to S3
    file_name: str = f'store_json/{to_snake_case(named_table)}.json'

    s3_client.put_object(
        Bucket=Clients.S3_BUCKET_NAME,
        Key=file_name,
        Body=records
    )

    print(f'Writing to S3: \t\t S3://{Clients.S3_BUCKET_NAME}/{file_name}')
    print(f'completed in \t\t {time.time() - export_time:.4f} seconds. \n')


#
# Pickle Pandas DataFrame
#
def export_to_s3_as_dataframe(s3_client, df, named_table: str):
    """
    Orchestrates export steps, encoding, and adds performance timer to logging
    """
    export_time = time.time()

    # Write DF to S3
    file_name: str = f'store_dataframes/{to_snake_case(named_table)}.df'

    pickled_df_buffer = io.BytesIO()
    pickle.dump(df, pickled_df_buffer)

    s3_client.put_object(
        Bucket=Clients.S3_BUCKET_NAME,
        Key=file_name,
        Body=pickled_df_buffer.getvalue()
    )

    print(f'Writing to S3: \t\t S3://{Clients.S3_BUCKET_NAME}/{file_name}')
    print(f'completed in \t\t {time.time() - export_time:.4f} seconds. \n')


#
# Pickle Python Dictionary
#
def export_to_s3_as_pickle_dict(s3_client, mongo_client, named_table: str):
    """
    Orchestrates export steps, encoding, and adds performance timer to logging
    """

    # mongo_dumps does not respect latin1 encoding, so we will pickle as dicts instead.
    # I turned myself into a pickle Morty! I'm Pickle Dict!

    export_time = time.time()

    # Serialize records as pickled Python Dictionary
    table = mongo_client[named_table]
    dict_to_pickle: dict = dict()
    for record in table.find():
        dict_to_pickle[record['_id']] = record
    records = pickle.dumps(dict_to_pickle)

    # Write pickled Python Dictionaries to S3
    file_name: str = f'store_pickle_dicts/{to_snake_case(named_table)}.pickle'

    s3_client.put_object(
        Bucket=Clients.S3_BUCKET_NAME,
        Key=file_name,
        Body=records
    )

    print(f'Writing to S3: \t\t S3://{Clients.S3_BUCKET_NAME}/{file_name}')
    print(f'completed in \t\t {time.time() - export_time:.4f} seconds. \n')


#
# Pickle Python Lists
#
def export_to_s3_as_pickle_list(s3_client, mongo_client, named_table: str):
    """
    Orchestrates export steps, encoding, and adds performance timer to logging
    """
    export_time = time.time()

    # Serialize records as pickled Python Dictionary
    table = mongo_client[named_table]
    list_to_pickle: list = [i for i in table.find()]
    records = pickle.dumps(list_to_pickle)

    # Write pickled Python Dictionaries to S3
    file_name: str = f'store_pickle_lists/{to_snake_case(named_table)}.pickle'

    s3_client.put_object(
        Bucket=Clients.S3_BUCKET_NAME,
        Key=file_name,
        Body=records
    )

    print(f'Writing to S3: \t\t S3://{Clients.S3_BUCKET_NAME}/{file_name}')
    print(f'completed in \t\t {time.time() - export_time:.4f} seconds. \n')

def export_to_s3_as_csv(s3_client, df: DataFrame, named_table: str, organization: str):
    """
    Orchestrates export steps, encoding, and adds performance timer to logging
    """
    export_time = time.time()

    # Write DF to S3
    file_name: str = f'clients/{organization}/data/FormResults/denormalized/{named_table}.csv'

    s3_client.put_object(
        Bucket=Clients.S3_OUTPUT_BUCKET,
        Key=file_name,
        Body=df.to_csv(index=False)
    )

    print(f'Writing to S3: \t\t S3://{Clients.S3_OUTPUT_BUCKET}/{file_name}')
    print(f'completed in \t\t {time.time() - export_time:.4f} seconds. \n')

if __name__ == '__main__':
    # export_to_s3_as_json(Clients.S3, Clients.MONGO, PuenteTables.FORM_SPECIFICATIONS)
    # export_to_s3_as_pickle_dict(Clients.S3, Clients.MONGO, PuenteTables.FORM_SPECIFICATIONS)
    export_to_s3_as_pickle_list(Clients.S3, Clients.MONGO, PuenteTables.FORM_SPECIFICATIONS)
