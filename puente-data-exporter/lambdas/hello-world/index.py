from mainRecords import mainRecords
from envHealth import envHealth
from evalMedical import evalMedical
from restCall import restCall

import sys
import json
import os

def handler(event, context=None):
  f = open(event, )
  event_json = json.load(f)
  survey_org = event_json["surveyingOrganization"]
  specifier = event_json["specifier"]
  # survey_org="Puente"
  # specifier = "SurveyData"
  print(survey_org)
  print(specifier)

  data = restCall(specifier, survey_org)

  if specifier == "SurveyData":
    response = mainRecords(data, survey_org)
  elif specifier == "HistoryEnvironmentalHealth":
    response = envHealth(data, survey_org)
  elif specifier == "EvaluationMedical":
    response = evalMedical(data, survey_org)
  else:
    print("Pick a valid specifier")

  return response

if __name__ == '__main__':
    globals()[sys.argv[1]](sys.argv[2])
