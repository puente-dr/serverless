from copy import Error
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__)))

from libs.envHealth import envHealth
from libs.evalMedical import evalMedical
from libs.mainRecords import mainRecords
from libs.vitals import vitals
from libs.assets import assets
from libs.assetSupplementary import assetSupplementary
from libs.customForms import customForms

from libs.restCall import restCall
from libs.utils import write_csv_to_s3


import pandas as pd

import libs.secretz as secretz

import json

def handler(event, context=None):
  try:
    if 'queryStringParameters' in event.keys():
      event = event["queryStringParameters"]

    survey_org = event["surveyingOrganization"]
    specifier = event["specifier"]
    custom_form_id = event["customFormId"] if "customFormId" in event.keys() else ""
  except Exception as error:
    print(f"Error: {error}")
    return {
      "headers": {"Access-Control-Allow-Origin":"*"},
      "statusCode": 400,
      "isBase64Encoded": False,
      "body": json.dumps({"error": "It appears your request is missing required parameters"})
    }

  try:
    primary_data, specifier_data = restCall(specifier, survey_org, custom_form_id)
  except Exception as error:
    print(f"Error: {error}")
    return {
      "headers": {"Access-Control-Allow-Origin":"*"},
      "statusCode": 400,
      "isBase64Encoded": False,
      "body": json.dumps({"error": "It appears your there was an error with your rest call to back4App"})
    }

  if specifier != "FormResults" and specifier != 'FormAssetResults':
    s3_bucket_key = 'clients/'+survey_org+'/data/'+specifier+'/'+specifier+'.csv'
  else:
    s3_bucket_key = 'clients/'+survey_org+'/data/'+specifier+'/'+specifier+'-'+custom_form_id+'.csv'
  
  try:
    if specifier in ["SurveyData", "HistoryEnvironmentalHealth", "EvaluationMedical", "Vitals", "FormResults"]:
      primary_data = mainRecords(primary_data)
    elif specifier in ["Assets", "FormAssetResults"]:
      primary_data = assets(primary_data)
    else:
      print("Invalid specifier")
      return {
        "headers": {"Access-Control-Allow-Origin":"*"},
        "statusCode": 400,
        "isBase64Encoded": False,
        "body": json.dumps({"error": "Your specifier parameter is invalid"})
      }
  except Exception as error:
    print(f"Error: {error}")
    return {
      "headers": {"Access-Control-Allow-Origin":"*"},
      "statusCode": 400,
      "isBase64Encoded": False,
      "body": json.dumps({"error": "There was an error cleaning the primary data"})
    }

  try:
    # cleaning specifier data
    if specifier == "HistoryEnvironmentalHealth":
      specifier_data = envHealth(specifier_data)
    elif specifier == "EvaluationMedical":
      specifier_data = evalMedical(specifier_data)
    elif specifier == "Vitals":
      specifier_data = vitals(specifier_data)
    elif specifier == "FormResults":
      specifier_data = customForms(specifier_data)
    elif specifier == 'FormAssetResults':
      specifier_data = assetSupplementary(specifier_data)
  except Exception as error:
    print(f"Error: {error}")
    return {
      "headers": {"Access-Control-Allow-Origin":"*"},
      "statusCode": 400,
      "isBase64Encoded": False,
      "body": json.dumps({"error": "There was an error cleaning the specifier data"})
    }

  try:
    if specifier_data is not None:
      data = pd.merge(specifier_data, primary_data, on="objectId")
      data = data.replace({pd.np.nan: ''})
    else:
      data = primary_data
  except Exception as error:
    print(f"Error: {error}")
    return {
      "headers": {"Access-Control-Allow-Origin":"*"},
      "statusCode": 400,
      "isBase64Encoded": False,
      "body": json.dumps({"error": "There was an error merging the primary and specifier data"})
    }

  try:
    url = write_csv_to_s3(data, s3_bucket_key)
  except Exception as error:
    print(f"Error: {error}")
    return {
      "headers": {"Access-Control-Allow-Origin":"*"},
      "statusCode": 400,
      "isBase64Encoded": False,
      "body": json.dumps({"error": "There was an error writing your data to s3"})
    }

  return {
    "headers": {"Access-Control-Allow-Origin":"*"},
    "statusCode": 200,
    "isBase64Encoded": False,
    "body": json.dumps({
      "s3_url": url
    })
  }
  