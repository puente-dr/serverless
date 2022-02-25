from load_to_s3 import export_to_s3_as_dataframe
from transform_form_specifications import get_custom_form_schema_df
from transform_form_results import get_custom_form_results_df
from utils.clients import Clients
from utils.constants import PuenteTables


def run_transform_jobs(event, context):
    """
    Orchestrate transformations
    """

    # Initialize AWS S3 Client
    s3_client = Clients.S3

    # if event.get(PuenteTables.ALLERGIES):
    # if event.get(PuenteTables.ASSETS):
    # if event.get(PuenteTables.EVALUATION_MEDICAL):
    # if event.get(PuenteTables.FORM_ASSET_RESULTS):
    if event.get(PuenteTables.FORM_RESULTS):
        df = get_custom_form_results_df()
        # export_to_s3_as_dataframe(s3_client, df, PuenteTables.FORM_RESULTS)

    if event.get(PuenteTables.FORM_SPECIFICATIONS):
        df = get_custom_form_schema_df()
        export_to_s3_as_dataframe(s3_client, df, PuenteTables.FORM_SPECIFICATIONS)

    # if event.get(PuenteTables.HISTORY_ENVIRONMENTAL_HEALTH):
    # if event.get(PuenteTables.HISTORY_MEDICAL):
    # if event.get(PuenteTables.OFFLINE_FORM):
    # if event.get(PuenteTables.OFFLINE_FORM_REQUEST):
    # if event.get(PuenteTables.HOUSEHOLD):
    # if event.get(PuenteTables.ROLE):
    # if event.get(PuenteTables.SESSION):
    # if event.get(PuenteTables.SURVEY_DATA):
    # if event.get(PuenteTables.USER):
    # if event.get(PuenteTables.VITALS):


if __name__ == '__main__':
    jobs = {
        PuenteTables.ALLERGIES: False,
        PuenteTables.ASSETS: False,
        PuenteTables.EVALUATION_MEDICAL: False,
        PuenteTables.FORM_ASSET_RESULTS: False,
        PuenteTables.FORM_RESULTS: True,
        PuenteTables.FORM_SPECIFICATIONS: False,
        PuenteTables.HISTORY_ENVIRONMENTAL_HEALTH: False,
        PuenteTables.HISTORY_MEDICAL: False,
        PuenteTables.OFFLINE_FORM: False,
        PuenteTables.OFFLINE_FORM_REQUEST: False,
        PuenteTables.HOUSEHOLD: False,
        PuenteTables.ROLE: False,
        PuenteTables.SESSION: False,
        PuenteTables.SURVEY_DATA: False,
        PuenteTables.USER: False,
        PuenteTables.VITALS: False
    }
    run_transform_jobs(jobs, {})
