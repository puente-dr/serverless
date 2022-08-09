import os
import sys; sys.path.append(os.path.join(os.path.dirname(__file__)))

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

def handler(event, context):
    # extract_back4app_data(Outputs.JSON)
    # extract_back4app_data(Outputs.PICKLE_DICT)
    extract_back4app_data(Outputs.PICKLE_LIST)
    run_transform_jobs(jobs, {})

    return

if __name__ == '__main__':
    handler()
