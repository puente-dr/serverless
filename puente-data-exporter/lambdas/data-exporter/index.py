import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__)))

from mainRecords import mainRecords
from envHealth import envHealth
from evalMedical import evalMedical
from restCall import restCall

def handler(event, context=None):
  survey_org = event["surveyingOrganization"]
  specifier = event["specifier"]
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
