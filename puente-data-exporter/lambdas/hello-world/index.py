from mainRecords import mainRecords
from envHealth import envHealth
from evalMedical import evalMedical
from restCall import restCall

def handler(event, context):
  survey_org = event["surveyingOrganization"]
  specifier = event["specifier"]

  data = restCall(specifier, survey_org)

  if specifier == "SurveyData":
    response = mainRecords(data)
  elif specifier == "HistoryEnvironmentalHealth":
    response = envHealth(data)
  elif specifier == "EvaluationMedical":
    response = evalMedical(data)
  else:
    print("Pick a valid specifier")

  return response
