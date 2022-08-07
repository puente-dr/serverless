from load_to_s3 import export_to_s3_as_dataframe, export_to_s3_as_csv
from transform_form_specifications import get_custom_form_schema_df
from transform_form_results import get_form_results_df
from utils.clients import Clients
from utils.constants import PuenteTables


def run_transform_jobs(event, context):
    """
    Orchestrate transformations
    """

    # Initialize AWS S3 Client
    s3_client = Clients.S3

    if event.get(PuenteTables.FORM_RESULTS):
        # raw_results == True does not aggregate the results, pass False to ensure aggregation
        df = get_form_results_df(raw_results=True)
        for org in df['form_result_surveying_organization'].unique():
            org_df = df[df['form_result_surveying_organization']==org]
            for custom_form in org_df['custom_form_id'].unique():
                final_df = org_df[org_df['custom_form_id'] == custom_form]
                export_to_s3_as_csv(s3_client, final_df, f"form-result-{custom_form}", org)

    if event.get(PuenteTables.FORM_SPECIFICATIONS):
        df = get_custom_form_schema_df()
        export_to_s3_as_dataframe(s3_client, df, PuenteTables.FORM_SPECIFICATIONS)

    # TODO: PUENTE FORMS
    # if event.get(PuenteTables.ALLERGIES):
    # if event.get(PuenteTables.FORM_ASSET_RESULTS):

    # if event.get(PuenteTables.ASSETS):
    # if event.get(PuenteTables.HISTORY_ENVIRONMENTAL_HEALTH):
    # if event.get(PuenteTables.HISTORY_MEDICAL):
    # if event.get(PuenteTables.SURVEY_DATA):

    # if event.get(PuenteTables.VITALS):
    # if event.get(PuenteTables.EVALUATION_MEDICAL):

    # if event.get(PuenteTables.OFFLINE_FORM):
    # if event.get(PuenteTables.OFFLINE_FORM_REQUEST):
    # if event.get(PuenteTables.HOUSEHOLD):
    # if event.get(PuenteTables.ROLE):
    # if event.get(PuenteTables.SESSION):
    # if event.get(PuenteTables.USER):