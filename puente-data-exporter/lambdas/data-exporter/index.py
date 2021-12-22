import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__)))

from libs.envHealth import envHealth
from libs.evalMedical import evalMedical
from libs.mainRecords import mainRecords
from libs.vitals import vitals
from libs.assets import assets
from libs.assetSupplementary import assetSupplementary

from libs.restCall import restCall
from libs.utils import write_csv_to_s3


import pandas as pd

import libs.secretz as secretz

import json

def handler(event, context=None):
  if 'queryStringParameters' in event.keys():
    event = event["queryStringParameters"]

  survey_org = event["surveyingOrganization"]
  specifier = event["specifier"]
  custom_form_id = event["customFormId"] if "customFormId" in event.keys() else ""

  primary_data, specifier_data = restCall(specifier, survey_org, custom_form_id)
  
  if specifier != "FormResults" and specifier != 'FormAssetResults':
    s3_bucket_key = 'clients/'+survey_org+'/data/'+specifier+'/'+specifier+'.csv'
  else:
    s3_bucket_key = 'clients/'+survey_org+'/data/'+specifier+'/'+specifier+'-'+custom_form_id+'.csv'
  
  # cleaning
  if specifier == "SurveyData":
    primary_data = mainRecords(primary_data)
  elif specifier == "HistoryEnvironmentalHealth":
    primary_data = mainRecords(primary_data)
    specifier_data = envHealth(specifier_data)
  elif specifier == "EvaluationMedical":
    primary_data = mainRecords(primary_data)
    specifier_data = evalMedical(specifier_data)
  elif specifier == "Vitals":
    primary_data = mainRecords(primary_data)
    specifier_data = vitals(specifier_data)
  elif specifier == 'Assets':
    primary_data = assets(primary_data)
  elif specifier == 'FormAssetResults':
    primary_data = assets(primary_data)
    specifier_data = assetSupplementary(specifier_data)

  if specifier_data is not None:
    data = pd.merge(primary_data, specifier_data, on="objectId")
    data = data.replace({pd.np.nan: ''})
  else:
    data = primary_data

  url = write_csv_to_s3(data, s3_bucket_key)

  response = {
    "s3_url": url
  }

  return {
    "headers": {"Access-Control-Allow-Origin":"*"},
    "statusCode": 200,
    "isBase64Encoded": False,
    "body": json.dumps(response)
  }
