from mainRecords import mainRecords
from envHealth import envHealth
from evalMedical import evalMedical

def handler(event, context):
  survey_org = event["surveyingOrganization"]
  specifier = event["specifier"]

  if specifier == "SurveyData":
    response = mainRecords(survey_org)
  elif specifier == "HistoryEnvironmentalHealth":
    response = envHealth(survey_org)
  elif specifier == "EvaluationMedical":
    response = evalMedical(survey_org)
  else:
    print("Pick a valid specifier")

  return response
