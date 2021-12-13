from typing import Dict
import requests
import json
from pandas import json_normalize
import pandas as pd
from time import gmtime, strftime

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__)))

import secretz

def restCall(specifier, survey_org, custom_form_id, url="https://parseapi.back4app.com/classes/"):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """
    print("2-A",strftime("%Y-%m-%d %H:%M:%S", gmtime()))
    # combine base url with specifier
    combined_url = url + specifier
    params = dict()

    if specifier != 'FormResults':
        params = {
            "order": "-updatedAt", "limit": 200000, "where":{
                json.dumps({
                    "surveyingOrganization": {
                        "$in": [survey_org]
                    }
                })
            }
        }
    else:
        # custom forms need to ensure that the correct custom form results are returned
        params = {
            "order": "-updatedAt", "limit": 200000, "where":{
                json.dumps({
                    "surveyingOrganization": {
                        "$in": [survey_org]
                    },
                    "formSpecificationsId": {
                        "$in": [custom_form_id]
                    }
                })
            }
        }


    headers = {
        "Content-Type": "application/json",
        "X-Parse-Application-Id": secretz.APP_ID,
        "X-Parse-REST-API-Key": secretz.REST_API_KEY,
    }
    print("2-B",strftime("%Y-%m-%d %H:%M:%S", gmtime()))
    # GET operation
    response = requests.get(combined_url, params=params, headers=headers)
    print("2-C",strftime("%Y-%m-%d %H:%M:%S", gmtime()))
    # turn it into json
    json_obj = response.json()
    print("2-D",strftime("%Y-%m-%d %H:%M:%S", gmtime()))
    # normalize (ie flatten) data, turns it into a pandas df
    normalized = json_normalize(json_obj["results"])
    print("2-E",strftime("%Y-%m-%d %H:%M:%S", gmtime()))
    # join non surveyData forms to surveyData
    if specifier != 'SurveyData':
        combined_url = url + 'SurveyData'
        params = {
            "order": "-updatedAt", "limit": 200000, "where":{
                json.dumps({
                    "surveyingOrganization": {
                        "$in": [survey_org]
                    }
                })
            }
        }
        print("2-F",strftime("%Y-%m-%d %H:%M:%S", gmtime()))
        response_survey_data = requests.get(combined_url, params=params, headers=headers)
        json_obj_survey_data = response_survey_data.json()
        normalized_survey_data = json_normalize(json_obj_survey_data["results"])
        print("2-G",strftime("%Y-%m-%d %H:%M:%S", gmtime()))
        normalized = normalized.rename(columns= {'objectId':specifier+'Id','client.objectId':'objectId','surveyingUser':'surveyingUserSupplementary'})
        merged_df = pd.merge(normalized_survey_data, normalized, on="objectId")
        print("2-H",strftime("%Y-%m-%d %H:%M:%S", gmtime()))
        return merged_df


    return normalized
