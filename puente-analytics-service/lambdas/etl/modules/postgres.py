import os
import datetime

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__)))

from create_drop import drop_tables, initialize_tables
from env_utils import NOSQL_TABLES
from utils import query_bronze_layer

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
)

from facts import add_nosql_to_fact, get_custom_forms


def fill_tables():
    survey_df = query_bronze_layer("SurveyData")
    print("survey df")
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

    for table_name, table_desc in NOSQL_TABLES.items():
        now = datetime.datetime.now()
        print("table name, desc")
        print(table_name, table_desc)
        add_nosql_to_forms(table_name, table_desc, now)
        print("add nosql to forms")
        ingest_nosql_table_questions(table_name)
        print("insert qs")

        add_nosql_to_fact(table_name, survey_df)
        print("add to fact")

    form_results = query_bronze_layer("FormResults")
    print("form results")
    form_results = form_results.merge(
        survey_df[["objectId", "communityname", "householdId"]].rename(
            {"householdId": "household", "objectId": "client.objectId"}, axis=1
        ),
        on="client.objectId",
    )
    get_custom_forms(form_results)
    print("custom forms")


def create_tables():
    initialize_tables()
    fill_tables()



drop_tables()
create_tables()
