from utils.clients import Clients
from utils.constants import Outputs
from load_to_s3 import \
    export_to_s3_as_json, \
    export_to_s3_as_pickle_dict, \
    export_to_s3_as_pickle_list


def extract_back4app_data(serialization: str, named_puente_table=None):
    """
    Parameters
    ----------
    serialization: Class Variable, Outputs, required
        Choose a data serialization method

    named_puente_table: Class Variable, PuenteTables class, optional
        Back4App Table Name

    Returns
    ------
    Logging

    """

    #
    # Initialize AWS S3 and MongoDB Clients
    #
    s3_client = Clients.S3
    mongo_client = Clients.MONGO

    # Marshal inputs to execute given one table or all tables as input
    if named_puente_table is None:
        all_tables = remove_back4app_tables(mongo_client.list_collection_names())
    else:
        all_tables = list(named_puente_table)

    #
    # Execute the extract and load to S3 based on output type
    #
    for table_name in all_tables:
        
        if serialization == Outputs.JSON:
            export_to_s3_as_json(s3_client, mongo_client, table_name)

        elif serialization == Outputs.PICKLE_DICT:
            export_to_s3_as_pickle_dict(s3_client, mongo_client, table_name)

        elif serialization == Outputs.PICKLE_LIST:
            export_to_s3_as_pickle_list(s3_client, mongo_client, table_name)

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
            if not any(i in t for i in [
                'B4a',
                'b4a',
                'offlineForm',
                'offlineFormRequest',
                'system.profile'
            ]):
                tables_to_keep.append(t)

    return tables_to_keep
