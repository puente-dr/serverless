import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__)))

from libs.envHealth import envHealth
from libs.evalMedical import evalMedical
from libs.mainRecords import mainRecords
from libs.restCall import restCall

import json
from dotenv import load_dotenv

def handler(event, context=None):
  if 'queryStringParameters' in event.keys():
    event = event["queryStringParameters"]

  load_dotenv(dotenv_path='./libs/.prod.env')
  
  bucket_name = event["bucket_name"]
  survey_org = event["surveyingOrganization"]
  specifier = event["specifier"]
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
    response = {"message": "Oops, look like you didnt inlude a valid specifier..."}
  
  return {
    "headers": {"Access-Control-Allow-Origin":"*"},
    "statusCode": 200,
    "isBase64Encoded": False,
    "body": json.dumps(response)
  }

# if __name__ == '__main__':
#     globals()[sys.argv[1]](sys.argv[2])
