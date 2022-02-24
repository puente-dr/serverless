import json
import os
import pprint
import sys; sys.path.append(os.path.join(os.path.dirname(__file__)))

import requests
import pandas as pd
from dotenv import load_dotenv; load_dotenv()
from tabulate import tabulate

from utils.helpers import shortuuid_random, to_snake_case


def get_custom_form_schema(custom_form_id: str):
    """
    Parameters
    ----------
    custom_form_id: string, required
        Custom Form ID, which is represented in the FormSpecificationsV2 class as "objectId"

    Returns
    ------
    Pandas dataframe with Custom Form Schema

    """

    #
    # Build Request URL, Headers, and Parameters
    #
    url = "https://parseapi.back4app.com/classes/FormSpecificationsV2"

    headers = {
        "Content-Type": "application/json",
        "X-Parse-Application-Id": os.getenv('PARSE_APP_ID'),
        "X-Parse-REST-API-Key": os.getenv('PARSE_REST_API_KEY'),
    }

    params = {
        "order": "-updatedAt",
        "limit": 200000,
        "where": {json.dumps({
            "objectId": {"$in": [custom_form_id]}}
        )}
    }

    #
    # Make REST API Call
    #
    response = requests.get(
        url,
        params=params,
        headers=headers
    )
    response.raise_for_status()

    # TODO: This gets the first item in a list of dictionaries and is intended
    #       to transform a single custom form. For bulk, we will want to refactor
    #       this to operate on all items in a list.
    response_json = response.json()['results'][0]

    # Denormalize Custom Form JSON
    form_df = denormalize_custom_form(response_json)
    questions_df = denormalize_questions(response_json)
    answers_df = denormalize_answers(questions_df)

    form_schema_df = form_df \
        .merge(questions_df, on='custom_form_id') \
        .drop(columns='question_options') \
        .merge(answers_df, on='question_id')

    print()
    print(tabulate(form_schema_df, headers=form_schema_df.columns))


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

    print()
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

    print()
    print('Denormalized Questions: ')

    print(tabulate(df, headers=cols_formatted))
    for i in cols_formatted:
        print(i)

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

    print()
    print('Denormalized Answers: ')
    print(tabulate(df, headers=cols_formatted))

    return df


def get_section_header(fields_dict):
    for item in fields_dict:
        if item.get('fieldType') == 'header':
            return item.get('label')


if __name__ == '__main__':
    # get_custom_form_schema('c570vfTSVy')
    get_custom_form_schema('QDJ0uNloic')
