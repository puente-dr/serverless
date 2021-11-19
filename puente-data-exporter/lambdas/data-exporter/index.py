import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__)))

from libs.envHealth import envHealth
from libs.evalMedical import evalMedical
from libs.mainRecords import mainRecords
from libs.restCall import restCall
from libs.utils import write_csv_to_s3

import json

def handler(event, context=None):
  if 'queryStringParameters' in event.keys():
    event = event["queryStringParameters"]

  bucket_name = event["bucket_name"]
  survey_org = event["surveyingOrganization"]
  specifier = event["specifier"]
  print(survey_org)
  print(specifier)

  data = restCall(specifier, survey_org)
  url = write_csv_to_s3(data, 'clients/'+survey_org+'/data/'+specifier+'/'+specifier+'.csv')

  # if specifier == "SurveyData":
  #   response = mainRecords(data, survey_org, bucket_name)
  # elif specifier == "HistoryEnvironmentalHealth":
  #   response = envHealth(data, survey_org, bucket_name)
  # elif specifier == "EvaluationMedical":
  #   response = evalMedical(data, survey_org, bucket_name)
  # else:
  #   response = {"message": "Oops, look like you didnt inlude a valid specifier..."}

  response = {
    "s3_url": url
  }

  return {
    "headers": {"Access-Control-Allow-Origin":"*"},
    "statusCode": 200,
    "isBase64Encoded": False,
    "body": json.dumps(response)
  }

# if __name__ == '__main__':
#     globals()[sys.argv[1]](sys.argv[2])
