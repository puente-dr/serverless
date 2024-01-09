import os
import datetime

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__)))

from create_drop import initialize_tables
from shared_modules.env_utils import NOSQL_TABLES
from shared_modules.utils import query_bronze_layer, explode_json

from dimensions import (
    get_community_dim,
    get_surveying_organization_dim,
    get_form_dim,
    get_users_dim,
    get_household_dim,
    get_patient_dim,
    get_question_dim,
    add_nosql_to_forms,
    ingest_nosql_table_questions,
    get_custom_form_questions
)

from facts import add_nosql_to_fact, get_custom_forms

def fill_tables(get_dimensions=True):
    survey_df = query_bronze_layer("SurveyData")
    print("survey df")
    if get_dimensions:
        get_community_dim(survey_df)
        print("community dim")
        get_surveying_organization_dim(survey_df)
        print("survey org dim")
        form_specs = query_bronze_layer("FormSpecificationsV2")
        print("form specs")
        get_form_dim(form_specs)
        print("form dim")
        users_df = query_bronze_layer("users")
        print("users")
        get_users_dim(users_df)
        print("users dim")
        get_household_dim(survey_df)
        print("household dim")
        get_patient_dim(survey_df)
        print("patient dim")
        get_question_dim(form_specs)
        print("question dim")

    # for table_name, table_desc in NOSQL_TABLES.items():
    #     now = datetime.datetime.now()
    #     print("table name, desc")
    #     print(table_name, table_desc)
    #     if get_dimensions:
    #         add_nosql_to_forms(table_name, table_desc, now)
    #         print("add nosql to forms")
    #         ingest_nosql_table_questions(table_name)
    #         print("insert qs")

    #     add_nosql_to_fact(table_name, survey_df)
    #     print("add to fact")

    form_results = query_bronze_layer("FormResults")
    form_results = form_results.merge(
        survey_df[["objectId", "communityname", "householdId"]].rename(
            {"householdId": "household", "objectId": "client.objectId"}, axis=1
        ),
        on="client.objectId",
    )
    form_results = explode_json(form_results)
    print(form_results.columns)
    print(form_results.head())
    print("form results")
    get_custom_form_questions(form_results)
    get_custom_forms(form_results)
    print("custom forms")


def create_tables():
    initialize_tables()
    fill_tables()
