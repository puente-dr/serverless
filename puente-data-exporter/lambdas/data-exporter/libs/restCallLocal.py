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

    denormalize_questions(response_json)


def denormalize_questions(data: dict):
    fields = data.get('fields')
    print('fields: ', fields)
    field_keys = []
    for field in fields:
        for fk in list(field.keys()):
            if fk not in field_keys:
                field_keys.append(fk)
    pprint.pprint(field_keys)

    rows = []
    for item in fields:
        row = [shortuuid_random()]
        for k in field_keys:
            row.append(item.get(k, ''))
        rows.append(row)

    # Prepend column for IDs
    cols = field_keys.insert(0, 'question_id')
    pprint.pprint(cols)

    df = pd.DataFrame(rows, columns=cols)
    print(tabulate(df))


if __name__ == '__main__':
    # FORM RESULTS PARAMS
    # specifier = 'FormResults'
    # survey_org = 'Puente'
    # custom_form_id = 'c570vfTSVy'

    # rest_call('FormResults', 'Puente', 'c570vfTSVy')
    # rest_call('FormResults', 'Cevicos', 'QDJ0uNloic')

    rest_call('FormSpecificationsV2', 'Cevicos', 'QDJ0uNloic')

