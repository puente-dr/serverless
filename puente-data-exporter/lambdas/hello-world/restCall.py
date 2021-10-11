import requests
import json
from pandas import json_normalize

import secretz

def lambda_handler(event, context):
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

    # try:
    #     ip = requests.get("http://checkip.amazonaws.com/")
    # except requests.RequestException as e:
    #     # Send some context about this error to Lambda Logs
    #     print(e)

    #     raise e
    # def restCall(specifier, url="https://parseapi.back4app.com/classes/"):
    # """Only difference between calls is the specifier
    #    This function makes a REST Get call to Back4App, returning a pandas df"""

    url="https://parseapi.back4app.com/classes/"
    specifier = "SurveyData"

    # combine base url with specifier
    combined_url = url + specifier

    # REST stuff to make sure you get data
    #HAD PROBLEMS WITH PAYLOAD SIZE, LOWERING LIMIT FIXED
    #increase payload?
    params = {
      "order": "-updatedAt", "limit": 200000, "where":{
          json.dumps({
            "surveyingOrganization": {
                "$in": ['OWS']
            }
          })
      }
}

    headers = {
        "Content-Type": "application/json",
        "X-Parse-Application-Id": secretz.APP_ID,
        "X-Parse-REST-API-Key": secretz.REST_API_KEY,
    }

    # GET operation
    response = requests.get(combined_url, params=params, headers=headers)

    # turn it into json
    json_obj = response.json()

    # normalize (ie flatten) data, turns it into a pandas df
    normalized = json_normalize(json_obj["results"])

    return json_obj
