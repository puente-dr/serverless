import json
import os
import pprint
import sys; sys.path.append(os.path.join(os.path.dirname(__file__)))

from dotenv import load_dotenv
import requests
import pandas as pd
load_dotenv()

pd.options.display.max_columns = 100
pd.options.display.max_colwidth = 400
pd.options.display.max_rows = 100


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

    # normalize (ie flatten) data, turns it into a pandas df
    print('RESPONSE JSON')
    response_json = response.json()['results'][0]
    #
    # cols = list(response_json.keys())
    # rows = []
    # for col in cols:
    #     rows.extend(str(response_json.get(col)))
    # df = pd.DataFrame(rows, columns=cols)

    fields = response_json.get('fields')
    field_keys = []
    for field in fields:
        for fk in list(field.keys()):
            if fk not in field_keys:
                field_keys.append(fk)
    pprint.pprint(field_keys)

    rows = []
    for item in fields:
        row = []
        for k in field_keys:
            row.append(item.get(k, ''))
        rows.append(row)

    df = pd.DataFrame(rows, columns=field_keys)
    print(df)

    # df = pd.json_normalize(response_json[0], max_level=1)
    # print(df)


    # normalized = pd.json_normalize(response_json)
    # print('NORMALIZED 1')
    # print(normalized)

    # # join non surveyData forms to surveyData
    # if specifier != "SurveyData" and specifier != "Assets":
    #     combined_url = (
    #         url + "SurveyData" if specifier != "FormAssetResults" else url + "Assets"
    #     )
    #     params = {
    #         **common_params,
    #         "where": {
    #             json.dumps(
    #                 {
    #                     "surveyingOrganization": {"$in": [survey_org]}
    #                 }
    #             )
    #         },
    #     }
    #
    #     response_primary = requests.get(combined_url, params=params, headers=headers)
    #     json_obj_primary = response_primary.json()
    #     print('RESPONSE JSON PRIMARY')
    #     pprint.pprint(json_obj_primary)
    #
    #     normalized_primary = json_normalize(json_obj_primary["results"])
    #     normalized = normalized.rename(
    #         columns={
    #             "objectId": "objectIdSupplementary",
    #             "client.objectId": "objectId",
    #             "surveyingUser": "surveyingUserSupplementary",
    #             "surveyingOrganization": "surveyingOrganizationSupplementary",
    #             "createdAt": "createdAtSupplementary",
    #         }
    #     )
    #     print('NORMALIZED 2')
    #     print(normalized.head())
    #
    #     print('NORMALIZED 3')
    #     print(normalized_primary.head())
    #
    #     return normalized_primary, normalized

    # return normalized, None


if __name__ == '__main__':
    # FORM RESULTS PARAMS
    # specifier = 'FormResults'
    # survey_org = 'Puente'
    # custom_form_id = 'c570vfTSVy'

    # rest_call('FormResults', 'Puente', 'c570vfTSVy')
    # rest_call('FormResults', 'Cevicos', 'QDJ0uNloic')

    rest_call('FormSpecificationsV2', 'Cevicos', 'QDJ0uNloic')

