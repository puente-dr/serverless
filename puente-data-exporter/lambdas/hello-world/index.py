import mainRecords
import envHealth

def handler(event, context):
  survey_org = event["surveyingOrganization"]
  specifier = event["specifier"]
  url = event["url"]

  if specifier == "SurveyData":
    response = mainRecords(survey_org)
  elif specifier == "HistoryEnvironmentalHealth":
    response = envHealth(survey_org)
  return response
