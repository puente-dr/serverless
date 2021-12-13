import sys
import os
from time import gmtime, strftime


sys.path.append(os.path.join(os.path.dirname(__file__)))

from libs.envHealth import envHealth
from libs.evalMedical import evalMedical
from libs.mainRecords import mainRecords
from libs.restCall import restCall
from libs.utils import write_csv_to_s3

import libs.secretz as secretz

import json

def handler(event, context=None):
  print("1",strftime("%Y-%m-%d %H:%M:%S", gmtime()))
  if 'queryStringParameters' in event.keys():
    event = event["queryStringParameters"]

  survey_org = event["surveyingOrganization"]
  specifier = event["specifier"]
  custom_form_id = event["customFormId"] if "customFormId" in event.keys() else ""
  print("2",strftime("%Y-%m-%d %H:%M:%S", gmtime()))

  data = restCall(specifier, survey_org, custom_form_id)
  
  print("3",strftime("%Y-%m-%d %H:%M:%S", gmtime()))
  if specifier != "FormResults":
    s3_bucket_key = 'clients/'+survey_org+'/data/'+specifier+'/'+specifier+'.csv'
  else:
    s3_bucket_key = 'clients/'+survey_org+'/data/'+specifier+'/'+specifier+'-'+custom_form_id+'.csv'
  
  print("4",strftime("%Y-%m-%d %H:%M:%S", gmtime()))
  url = write_csv_to_s3(data, s3_bucket_key)

  response = {
    "s3_url": url
  }

  print("5",strftime("%Y-%m-%d %H:%M:%S", gmtime()))
  return {
    "headers": {"Access-Control-Allow-Origin":"*"},
    "statusCode": 200,
    "isBase64Encoded": False,
    "body": json.dumps(response)
  }

# if __name__ == '__main__':
#     globals()[sys.argv[1]](sys.argv[2])
