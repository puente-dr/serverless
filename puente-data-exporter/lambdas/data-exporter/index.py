import json
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__)))
from libs.restCall import restCall
from libs.utils import write_csv_to_s3


def handler(event, context=None):
    if "queryStringParameters" in event.keys():
        event = event["queryStringParameters"]

    survey_org = event["surveyingOrganization"]
    specifier = event["specifier"]
    custom_form_id = event.get("customFormId", "")

    data = restCall(specifier, survey_org, custom_form_id)

    if specifier != "FormResults" and specifier != "FormAssetResults":
        s3_bucket_key = f"clients/{survey_org}/data/{specifier}/{specifier}.csv"
    else:
        s3_bucket_key = f"clients/{survey_org}/data/{specifier}/{specifier}-{custom_form_id}.csv"

    url = write_csv_to_s3(data, s3_bucket_key)

    response = {"s3_url": url}

    return {
        "headers": {"Access-Control-Allow-Origin": "*"},
        "statusCode": 200,
        "isBase64Encoded": False,
        "body": json.dumps(response),
    }
