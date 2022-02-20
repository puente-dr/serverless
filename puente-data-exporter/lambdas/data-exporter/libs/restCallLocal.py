import json
import os
import pprint
import string
import sys; sys.path.append(os.path.join(os.path.dirname(__file__)))

import requests
import pandas as pd
import shortuuid
from dotenv import load_dotenv; load_dotenv()
from tabulate import tabulate

pd.options.display.max_columns = 100
pd.options.display.max_colwidth = 40
pd.options.display.max_rows = 100

alphabet = string.ascii_lowercase + string.digits
su = shortuuid.ShortUUID(alphabet=alphabet)


def shortuuid_random():
    return su.random(length=8)


def rest_call(
    specifier, survey_org, custom_form_id, url="https://parseapi.back4app.com/classes/"
):
    """
    Parameters
    ----------
    specifier: string, required
        Parse model to query

    survey_org: string, required
        Organization to query results for

    custom_form_id: string, not required
        Custom form object id

    url: string, required
        Parse endpoint
    Returns
    ------
    Pandas dataframe with query results

    """

    #
    # Build Request URL, Headers, and Parameters
    #
    combined_url = url + specifier

    headers = {
        "Content-Type": "application/json",
        "X-Parse-Application-Id": os.getenv('PARSE_APP_ID'),
        "X-Parse-REST-API-Key": os.getenv('PARSE_REST_API_KEY'),
    }

    common_params = {
        "order": "-updatedAt",
        "limit": 200000,
    }

    params = {}

    # if specifier != "FormResults" and specifier != "FormAssetResults":
    #     params = {
    #         **common_params,
    #         "where": {json.dumps({
    #             "surveyingOrganization": {"$in": [survey_org]}}
    #         )},
    #     }
    # else:
    #     # custom forms need to ensure that the correct custom form results are returned
    #     params = {
    #         **common_params,
    #         "where": {json.dumps({
    #             "surveyingOrganization": {"$in": [survey_org]},
    #             "formSpecificationsId": {"$in": [custom_form_id]}}
    #         )}
    #     }

    if specifier == 'FormSpecificationsV2':
        params = {
            **common_params,
            "where": {json.dumps({
                "objectId": {"$in": [custom_form_id]}}
            )}
        }

    response = requests.get(
        combined_url,
        params=params,
        headers=headers
    )
    response.raise_for_status()

    print('RESPONSE JSON')
    response_json = response.json()['results'][0]

    questions_df = denormalize_questions(response_json)

    answers_df = denormalize_answers(questions_df)

    print('len(questions_df) ', len(questions_df))
    print('len(answers_df) ', len(answers_df))

    form_schema_df = questions_df \
        .drop(columns='question_options') \
        .merge(answers_df, on='question_id')

    print(tabulate(form_schema_df, headers=form_schema_df.columns))


def denormalize_questions(data: dict):
    fields = data.get('fields')

    cols = []
    for field in fields:
        for fk in list(field.keys()):
            if fk not in cols:
                cols.append(fk)

    header = get_section_header(fields)

    rows = []
    for item in fields:
        row = [header, shortuuid_random()]
        for col in cols:
            row.append(item.get(col))
        rows.append(row)

    # Prepend column for IDs and section Header
    # Format column names and add columns for Question ID and Answer ID
    cols_renamed = [f'question_{i}' for i in cols]
    cols_renamed.insert(0, 'form_header')
    cols_renamed.insert(1, 'question_id')

    df = pd.DataFrame(rows, columns=cols_renamed)

    # print('Denormalized Questions: ')
    # print(tabulate(df, headers=cols_renamed))

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
    pprint.pprint(cols_renamed)

    df = pd.DataFrame(rows, columns=cols_renamed)

    # print('Denormalized Answers: ')
    # print(tabulate(df, headers=cols_renamed))

    return df


def get_section_header(fields_dict):
    for item in fields_dict:
        if item.get('fieldType') == 'header':
            return item.get('label')


if __name__ == '__main__':
    # FORM RESULTS PARAMS
    # specifier = 'FormResults'
    # survey_org = 'Puente'
    # custom_form_id = 'c570vfTSVy'

    # rest_call('FormResults', 'Puente', 'c570vfTSVy')
    # rest_call('FormResults', 'Cevicos', 'QDJ0uNloic')

    rest_call('FormSpecificationsV2', 'Cevicos', 'QDJ0uNloic')
