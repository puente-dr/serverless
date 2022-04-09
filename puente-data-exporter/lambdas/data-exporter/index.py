import json
import os
import sys; sys.path.append(os.path.join(os.path.dirname(__file__)))

import pandas as pd

from libs.assets import assets
from libs.assetSupplementary import assetSupplementary
from libs.customForms import customForms
from libs.envHealth import envHealth
from libs.evalMedical import evalMedical
from libs.mainRecords import mainRecords
from libs.restCall import restCall
from libs.utils import write_csv_to_s3
from libs.vitals import vitals


common_req = {
    "headers": {"Access-Control-Allow-Origin": "*"},
    "isBase64Encoded": False,
}


def handler(event, context):
    # TODO: I would recommend using a logger instead of forcing error messages through try/except loops.
    #       Try/except loops are heavy handed and IME are particularly difficult to debug in remote Lambda executions.
    #       They can also run up your AWS bill if Lambda retries are not configured carefully.
    #       I've refactored this logic with if/else loops in the Lambda handler here, but noticed several elsewhere.

    #
    # Parse request parameters
    #
    params = event.get("queryStringParameters")
    if params is not None:
        survey_org = params["surveyingOrganization"]
        specifier = params["specifier"]
        custom_form_id = params.get("customFormId", "")
    else:
        return err_msg("It appears your request is missing required parameters")

    #
    # Make REST call to back4app
    #
    primary_data, specifier_data = restCall(specifier, survey_org, custom_form_id)

    # TODO: Repeatedly overwriting these objects in the remainder of this script is confusing

    #
    # Create S3 Bucket Key
    #
    s3_bucket_key = f"clients/{survey_org}/data/{specifier}/{specifier}.csv"
    if specifier in ["FormResults", "FormAssetResults"]:
        s3_bucket_key = f"clients/{survey_org}/data/{specifier}/{specifier}-{custom_form_id}.csv"

    #
    # Clean Primary Data and assets
    #
    if specifier in [
        "SurveyData",
        "HistoryEnvironmentalHealth",
        "EvaluationMedical",
        "Vitals",
        "FormResults",
    ]:
        primary_data = mainRecords(primary_data)
    elif specifier in ["Assets", "FormAssetResults"]:
        primary_data = assets(primary_data)
    else:
        return err_msg("Your specifier parameter is invalid or there was an error cleaning the primary data")

    #
    # Cleaning Specifier Data
    #
    if specifier == "HistoryEnvironmentalHealth":
        specifier_data = envHealth(specifier_data)
    elif specifier == "EvaluationMedical":
        specifier_data = evalMedical(specifier_data)
    elif specifier == "Vitals":
        specifier_data = vitals(specifier_data)
    elif specifier == "FormResults":
        specifier_data = customForms(specifier_data)
    elif specifier == "FormAssetResults":
        specifier_data = assetSupplementary(specifier_data)
    else:
        return err_msg("There was an error cleaning the specifier data")

    #
    # Joining Clean Data
    #
    if specifier_data is not None:
        data = pd.merge(specifier_data, primary_data, on="objectId")
        data = data.replace({pd.np.nan: ""})
    else:
        return err_msg("There was an error cleaning the specifier data")

    #
    # Write Data to S3
    #
    if data is not None:
        url = write_csv_to_s3(data, s3_bucket_key)
    else:
        url = write_csv_to_s3(primary_data, s3_bucket_key)
        print("There was an error merging the primary and specifier data. Falling back to Primary Data.")

    return {
        **common_req,
        "statusCode": 200,
        "body": json.dumps({"s3_url": url}),
    }


def err_msg(msg: str) -> dict:
    return {
        **common_req,
        "statusCode": 400,
        "body": json.dumps({"error": msg})
    }


if __name__ == '__main__':
    handler()