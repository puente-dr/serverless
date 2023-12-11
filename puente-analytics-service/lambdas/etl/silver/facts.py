import pandas as pd
import numpy as np
import json
import uuid
from psycopg2.errors import ForeignKeyViolation

from shared_modules.utils import (
    get_subquestions,
    connection,
    md5_encode,
    parse_json_config,
    query_bronze_layer,
)
from shared_modules.env_utils import CONFIGS, CSV_PATH


def get_custom_forms(df):
    con = connection()
    cur = con.cursor()

    fk_missing_rows = []
    missing_qa_rows = []
    df["fields"] = df["fields"].apply(json.loads)
    exploded_df = df.explode("fields")

    exploded_df["fields"] = exploded_df["fields"].apply(lambda x: get_subquestions(x))
    exploded_df = exploded_df.explode("fields")

    exploded_df["title"] = exploded_df["fields"].apply(lambda x: x.get("title"))
    exploded_df["question_answer"] = exploded_df["fields"].apply(
        lambda x: x.get("answer")
    )

    missing_dict = {"hhids": [], "comms": [], "answers": [], "users": []}

    for i, row in exploded_df.iterrows():
        object_id = row.get("objectId")
        user = row.get("surveyingUser")
        survey_org = row.get("surveyingOrganization")
        form = row.get("formSpecificationsId")
        created_at = row.get("createdAt")
        updated_at = row.get("updatedAt")
        household = row.get("household")
        community_name = row.get("communityname")
        title = row.get("title")
        question_answer = row.get("question_answer")

        if title == "appVersion":
            continue

        row_insert = (
            object_id,
            user,
            survey_org,
            form,
            household,
            community_name,
            title,
            question_answer,
        )
        check_list = []
        for field in [household, community_name, question_answer, user]:
            if isinstance(field, str):
                check = "test" in field.lower()
                check_list.append(check)
        if any(check_list):
            continue

        if household in [None, np.nan]:
            household_id = None
        else:
            household_id = md5_encode(household)

        if community_name in [None, np.nan]:
            missing_dict["comms"].append(row_insert)
            continue

        if question_answer in [None, np.nan]:
            missing_dict["answers"].append(row_insert)
            continue

        if user in [None, np.nan]:
            missing_dict["users"].append(row_insert)
            continue

        patient_id = md5_encode(object_id)
        user_id = md5_encode(user)
        surveying_organization_id = md5_encode(survey_org)
        form_id = md5_encode(form)
        community_id = md5_encode(community_name)

        question_id = md5_encode(title)

        id = str(uuid.uuid4())

        insert_tuple = (
            id,
            surveying_organization_id,
            user_id,
            community_id,
            question_id,
            question_answer,
            created_at,
            updated_at,
            patient_id,
            household_id,
            form_id,
        )

        try:
            cur.execute(
                f"""
            INSERT INTO survey_fact (uuid, surveying_organization_id, surveying_user_id, community_id, question_id, question_answer, created_at, updated_at, patient_id, household_id, form_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    id,
                    surveying_organization_id,
                    user_id,
                    community_id,
                    question_id,
                    question_answer,
                    created_at,
                    updated_at,
                    patient_id,
                    household_id,
                    form_id,
                ),
            )

        except ForeignKeyViolation:
            insert_tuple = tuple(
                list(insert_tuple)[:5] + [title, object_id] + list(insert_tuple[5:])
            )
            fk_missing_rows.append(insert_tuple)
            cur.execute("ROLLBACK")
            continue

    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()

    cols = [
        "object_id",
        "user",
        "survey_org",
        "form",
        "household",
        "community",
        "question",
        "question_answer",
    ]

    for table, missing in missing_dict.items():
        missing_df = pd.DataFrame.from_records(missing, columns=cols)
        if missing_df.shape[0] > 0:
            missing_df.to_csv(f"{CSV_PATH}/customforms_missing_{table}.csv", index=False)

    cols = [
        "uuid",
        "surveying_organization_id",
        "user_id",
        "community_id",
        "question_id",
        "question_title",
        "objectId",
        "question_answer",
        "created_at",
        "updated_at",
        "patient_id",
        "household_id",
        "form_id",
    ]

    fk_missing_rows_df = pd.DataFrame.from_records(fk_missing_rows, columns=cols)
    if fk_missing_rows_df.shape[0] > 0:
        fk_missing_rows_df.to_csv(f"{CSV_PATH}/custom_fk.csv", index=False)
    missing_qa_rows_df = pd.DataFrame.from_records(missing_qa_rows, columns=cols)
    if missing_qa_rows_df.shape[0] > 0:
        missing_qa_rows_df.to_csv(f"{CSV_PATH}/custom_nullqa.csv", index=False)

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"questions": " "}),
        "isBase64Encoded": False,
    }


def add_nosql_to_fact(table_name, survey_df):
    con = connection()
    cur = con.cursor()
    rename_dict = {
        "objectId": "objectIdSupplementary",
        "client.objectId": "objectId",
        "surveyingUser": "surveyingUserSupplementary",
        "surveyingOrganization": "surveyingOrganizationSupplementary",
        "createdAt": "createdAtSupplementary",
        "updatedAt": "updatedAtSuplementary",
        "yearsLivedinthecommunity": "yearsLivedinthecommunitySupplementary",
        "yearsLivedinThisHouse": "yearsLivedinThisHouseSupplementary",
        "waterAccess": "waterAccessSupplementary",
        "numberofIndividualsLivingintheHouse": "numberofIndividualsLivingintheHouseSupplementary",
    }

    nosql_df = query_bronze_layer(table_name).rename(rename_dict, axis=1)
    id_cols = [
        "objectId",
        "surveyingOrganization",
        "surveyingUser",
        "communityname",
        "householdId",
        "createdAt",
        "updatedAt",
    ]
    merged = survey_df.merge(nosql_df, on="objectId")

    config = parse_json_config(CONFIGS[table_name])
    questions = []
    for question_tuple in config:
        _, _, formik_key, _, _ = question_tuple
        questions.append(formik_key)

    questions = [question for question in questions if question in list(merged.columns)]
    comb_df = merged[id_cols + questions].melt(
        id_vars=id_cols, var_name="question", value_name="answer"
    )

    ignore_questions = ["searchIndex"] + [col for col in questions if "location" in col]

    fk_missing_rows = []
    notnull_missing_rows = []

    missing_dict = {"hhids": [], "users": [], "comms": [], "answers": []}

    for _, row in comb_df.iterrows():
        created_at = row["createdAt"]
        updated_at = row["updatedAt"]
        question_name = row["question"]
        if question_name in ignore_questions:
            continue
        question_answer = row["answer"]

        object_id = row["objectId"]
        survey_org = row["surveyingOrganization"]
        user = row["surveyingUser"]
        community_name = row["communityname"]
        nosql_household_id = row["householdId"]

        check_list = []
        for field in [nosql_household_id, user, community_name]:
            if isinstance(field, str):
                check = "test" in field.lower()
                check_list.append(check)
        if any(check_list):
            continue

        row_insert = (
            object_id,
            survey_org,
            user,
            community_name,
            nosql_household_id,
            question_name,
            question_answer,
            table_name,
        )
        if nosql_household_id in [None, np.nan]:
            household_id = None
        else:
            household_id = md5_encode(nosql_household_id)
        if user in [None, np.nan]:
            missing_dict["users"].append(row_insert)
            continue
        if community_name in [None, np.nan]:
            missing_dict["comms"].append(row_insert)
            continue
        if question_answer in [None, np.nan]:
            missing_dict["answers"].append(row_insert)
            continue
        patient_id = md5_encode(object_id)
        surveying_organization_id = md5_encode(survey_org)
        user_id = str(md5_encode(user))
        community_id = md5_encode(community_name)
        question_id = md5_encode(question_name)
        form_id = md5_encode(table_name)
        id = str(uuid.uuid4())

        insert_tuple = (
            id,
            surveying_organization_id,
            user_id,
            community_id,
            question_id,
            question_answer,
            created_at,
            updated_at,
            patient_id,
            household_id,
            form_id,
        )
        try:
            cur.execute(
                f"""
            INSERT INTO survey_fact (uuid, surveying_organization_id, surveying_user_id, community_id, question_id, question_answer, created_at, updated_at, patient_id, household_id, form_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    id,
                    surveying_organization_id,
                    user_id,
                    community_id,
                    question_id,
                    question_answer,
                    created_at,
                    updated_at,
                    patient_id,
                    household_id,
                    form_id,
                ),
            )

        except ForeignKeyViolation:
            fk_missing_rows.append(insert_tuple)
            cur.execute("ROLLBACK")
            continue

    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()

    cols = [
        "uuid",
        "surveying_organization_id",
        "user_id",
        "community_id",
        "question_id",
        "question_answer",
        "created_at",
        "updated_at",
        "patient_id",
        "household_id",
        "form_id",
    ]

    notnull_missing_rows_df = pd.DataFrame.from_records(
        notnull_missing_rows, columns=cols
    )
    fk_missing_rows_df = pd.DataFrame.from_records(fk_missing_rows, columns=cols)

    cols = [
        "object_id",
        "survey_org",
        "user",
        "community_name",
        "household_id",
        "question_name",
        "question_answer",
        "table_name",
    ]

    for table, missing in missing_dict.items():
        missing_df = pd.DataFrame.from_records(missing, columns=cols)
        if missing_df.shape[0] > 0:
            missing_df.to_csv(
                f"{CSV_PATH}/add_nosql_to_fact_{table_name}_missing_{table}.csv", index=False
            )

    if notnull_missing_rows_df.shape[0] > 0:
        notnull_missing_rows_df.to_csv(
            f"{CSV_PATH}/add_nosql_to_fact_notnull_{table_name}.csv", index=False
        )
    if fk_missing_rows_df.shape[0] > 0:
        fk_missing_rows_df.to_csv(
            f"{CSV_PATH}/add_nosql_to_fact_fk_{table_name}.csv", index=False
        )

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"questions": comb_df.to_json()}),
        "isBase64Encoded": False,
    }
