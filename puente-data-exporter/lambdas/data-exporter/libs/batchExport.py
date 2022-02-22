import json
import os
import pprint
import re
import string
import sys; sys.path.append(os.path.join(os.path.dirname(__file__)))

import requests
import pandas as pd
from pymongo import MongoClient
import shortuuid
from dotenv import load_dotenv; load_dotenv()
from tabulate import tabulate

alphabet = string.ascii_lowercase + string.digits
su = shortuuid.ShortUUID(alphabet=alphabet)


class PuenteTables:
    ROLE = 'Role'
    SESSION = 'Session'
    USER = 'User'
    ALLERGIES = 'Allergies'
    ASSETS = 'Assets'
    B4A_CUSTOM_FIELD = 'B4aCustomField'
    B4A_MENU_ITEM = 'B4aMenuItem'
    B4A_SETTING = 'B4aSetting'
    EVALUATION_MEDICAL = 'EvaluationMedical'
    FORM_ASSET_RESULTS = 'FormAssetResults'
    FORM_RESULTS = 'FormResults'
    FORM_SPECIFICATIONS = 'FormSpecificationsV2'
    HISTORY_ENVIRONMENTAL_HEALTH = 'HistoryEnvironmentalHealth'
    HISTORY_MEDICAL = 'HistoryMedical'
    HOUSEHOLD = 'Household'
    SURVEY_DATA = 'SurveyData'
    VITALS = 'Vitals'
    OFFLINE_FORM = 'offlineForm'
    OFFLINE_FORM_REQUEST = 'offlineFormRequest'


def init_mongo_connection():
    # Initialize MongoDB Client
    client = MongoClient(os.getenv('DATABASE_URI'))
    print('\n\n')
    print('Mongo Client...')
    print(client)
    # pprint.pprint(vars(client))

    # Make Connection to Database
    #
    # The name that Back4App gives its databases can be found
    # after the last slash in MongoDB URI string
    db_name = client.get_default_database().name
    db = client[db_name]
    print('\n\n')
    print('Default Database...')
    print(db)
    # pprint.pprint(vars(db))

    collections_list = db.list_collection_names()

    for table_name in collections_list:
        # Use this as limiter during development
        if table_name == PuenteTables.FORM_SPECIFICATIONS:
            with open(f'{table_name}.json', 'wb') as f:
                f.write()

            table = db[table_name]
            for item in table.find():
                print(list(item.keys()))
                print()

    table = db[PuenteTables.FORM_SPECIFICATIONS]
    print('\n\n')
    print('Form Specifications Table...')
    print(table)

    print('\n\n')
    print('Items in Form Specifications Table...')
    for item in table.find():
        print(list(item.keys()))
        print()


# def export_named_table(puente_table: str, mongo_db):
#
#     collections_list = mongo_db.list_collection_names()
#     table = (c for c in collections_list if c == puente_table)
#     print
#
#
#     for c in collections_list:
#         print(c)

    # db_names = client.list_database_names()
    # pprint.pprint(db_names)

    #
    # print('Mongo Table...')
    # table = mc[PuenteTables.FORM_SPECIFICATIONS]
    # print(type(table), table)
    # # pprint.pprint(vars(table))
    #
    # q = table.find_one({'objectId': 'QDJ0uNloic'})
    # print('q: ', q)
    # pprint.pprint(vars(q))

    # print('Mongo Database...')
    # db = mc['puente-staging']
    # pprint.pprint(vars(db))
    #
    # print('\n\n LIST COLLECTION NAMES')
    # print(db.list_collection_names())


# def export_mongodb():


def batch_export(puente_table):
    """
    Parameters
    ----------
    puente_table: Class Variable, PuenteTables class, required
        Back4App Table Name

    Returns
    ------
    Results from table

    """

    #
    # Build Request URL, Headers, and Parameters
    #
    url = f"https://parseapi.back4app.com/classes/{puente_table}"

    headers = {
        "Content-Type": "application/json",
        "X-Parse-Application-Id": os.getenv('PARSE_APP_ID'),
        "X-Parse-REST-API-Key": os.getenv('PARSE_REST_API_KEY'),
    }

    params = {
        "order": "-updatedAt",
        "limit": 200000,
        # "where": {json.dumps({
        #     "objectId": {"$in": [custom_form_id]}}
        # )}
    }

    #
    # Make REST API Call
    #
    response = requests.get(
        url,
        # params=params,
        headers=headers
    )
    response.raise_for_status()

    print(f'Response Status: {response.status_code}')
    print(f'Next Page: {str(response.next)}')

    response_json = response.json()['results']
    print(f'Number of items in {puente_table}: {len(response_json)}')

    print('RESPONSE JSON')
    pprint.pprint(response_json)

    #
    # Denormalize Custom Form JSON
    #
    # form_df = denormalize_custom_form(response_json)
    # questions_df = denormalize_questions(response_json)
    # answers_df = denormalize_answers(questions_df)
    #
    # form_schema_df = form_df \
    #     .merge(questions_df, on='custom_form_id') \
    #     .drop(columns='question_options') \
    #     .merge(answers_df, on='question_id')
    #
    # print(tabulate(form_schema_df, headers=form_schema_df.columns))


def denormalize_custom_form(data: dict):

    # Remove fields, which are denormalized elsewhere, and location, which we do not need
    form_data = data.copy()
    form_data.pop('fields')
    form_data.pop('location')

    df = pd.json_normalize(form_data) \
        .rename(columns={'objectId': 'id'}) \
        .add_prefix('custom_form_')

    # Rename and snake_case columns
    cols_dict = dict(zip(
        list(df.columns),
        to_snake_case(list(df.columns))
    ))
    df = df.rename(columns=cols_dict)

    print('Denormalized Custom Form: ')
    print(tabulate(df, headers=df.columns))

    return df


def denormalize_questions(data: dict):
    fields = data.get('fields')
    custom_form_id = data.get('objectId')

    cols = []
    for field in fields:
        for fk in list(field.keys()):
            if fk not in cols:
                cols.append(fk)

    header = get_section_header(fields)

    rows = []
    for item in fields:
        row = [custom_form_id, header, shortuuid_random()]
        for col in cols:
            row.append(item.get(col))
        rows.append(row)

    # Prepend column for IDs and section Header
    # Format column names and add columns for Question ID and Answer ID
    cols_renamed = [f'question_{i}' for i in cols]
    cols_renamed.insert(0, 'custom_form_id')
    cols_renamed.insert(1, 'custom_form_header')
    cols_renamed.insert(2, 'question_id')
    cols_formatted = to_snake_case(cols_renamed)

    df = pd.DataFrame(rows, columns=cols_formatted)

    # print('Denormalized Questions: ')
    # print(tabulate(df, headers=cols_formatted))

    return df


def denormalize_answers(questions_df):
    tmp_df = questions_df[['question_id', 'question_options']]

    # Exclude fieldTypes "header" and "numberInput"
    tmp_df = tmp_df[tmp_df['question_options'].notnull()]

    cols = []
    for options_list in tmp_df['question_options']:
        if options_list is not None:
            for option in options_list:
                # Get cols
                for fk in list(option.keys()):
                    if fk not in cols:
                        cols.append(fk)

    rows = []
    for q_id, options_list in zip(tmp_df['question_id'], tmp_df['question_options']):
        if options_list is not None:
            for option in options_list:
                row = [q_id, shortuuid_random()]
                for col in cols:
                    row.append(option.get(col))
                rows.append(row)

    # Format column names and add columns for Question ID and Answer ID
    cols_renamed = [f'answer_{i}' for i in cols]
    cols_renamed.insert(0, 'question_id')
    cols_renamed.insert(1, 'answer_id')
    cols_formatted = to_snake_case(cols_renamed)

    df = pd.DataFrame(rows, columns=cols_formatted)

    # print('Denormalized Answers: ')
    # print(tabulate(df, headers=cols_formatted))

    return df


def shortuuid_random():
    return su.random(length=8)


def to_snake_case(cols: list) -> list:
    return [
        re.sub(r'(?<!^)(?=[A-Z])', '_', col).lower()
        for col in cols
    ]


def get_section_header(fields_dict):
    for item in fields_dict:
        if item.get('fieldType') == 'header':
            return item.get('label')


if __name__ == '__main__':
    init_mongo_connection()
    # batch_export(PuenteTables.HISTORY_MEDICAL)
