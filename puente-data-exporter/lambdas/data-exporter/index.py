import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__)))

from mainRecords import mainRecords
from envHealth import envHealth
from evalMedical import evalMedical
from restCall import restCall

#import json

import pandas as pd

import boto3 #don't need this in lambda, but might to run locally idk


def handler(event, context=None):
  # f = open(event, )
  # event_json = json.load(f)

  bucket_name = event["bucket_name"]
  survey_org = event["surveyingOrganization"]
  specifier = event["specifier"]
  # survey_org="Puente"
  # specifier = "SurveyData"
  print(survey_org)
  print(specifier)

  data = restCall(specifier, survey_org)

  if specifier == "SurveyData":
    response = mainRecords(data, survey_org, bucket_name)
  elif specifier == "HistoryEnvironmentalHealth":
    response = envHealth(data, survey_org, bucket_name)
  elif specifier == "EvaluationMedical":
    response = evalMedical(data, survey_org, bucket_name)
  else:
    print("Pick a valid specifier")
    response = {
      "status": 200,
      "message": "Oops, looks like you didnt include a valid specifier.."
    }

  return response

# if __name__ == '__main__':
#     globals()[sys.argv[1]](sys.argv[2])
