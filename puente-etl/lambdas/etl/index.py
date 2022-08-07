from utils.constants import Outputs, PuenteTables
from extract_back4app_data import extract_back4app_data
from jobs import run_transform_jobs

jobs = {
        PuenteTables.FORM_SPECIFICATIONS: True,
        PuenteTables.FORM_RESULTS: True,
        PuenteTables.ALLERGIES: False,
        PuenteTables.ASSETS: False,
        PuenteTables.EVALUATION_MEDICAL: False,
        PuenteTables.FORM_ASSET_RESULTS: False,
        PuenteTables.HISTORY_ENVIRONMENTAL_HEALTH: False,
        PuenteTables.HISTORY_MEDICAL: False,
        #PuenteTables.OFFLINE_FORM: False,
        #PuenteTables.OFFLINE_FORM_REQUEST: False,
        PuenteTables.HOUSEHOLD: False,
        PuenteTables.ROLE: False,
        PuenteTables.SESSION: False,
        PuenteTables.SURVEY_DATA: False,
        PuenteTables.USER: False,
        PuenteTables.VITALS: False
    }

common_req = {
    "headers": {"Access-Control-Allow-Origin": "*"},
    "isBase64Encoded": False,
}


def handler():
    # extract_back4app_data(Outputs.JSON)
    # extract_back4app_data(Outputs.PICKLE_DICT)
    extract_back4app_data(Outputs.PICKLE_LIST)
    run_transform_jobs(jobs, {})

    return



# def err_msg(msg: str) -> dict:
#     return {
#         **common_req,
#         "statusCode": 400,
#         "body": json.dumps({"error": msg})
#     }


if __name__ == '__main__':
    handler()
