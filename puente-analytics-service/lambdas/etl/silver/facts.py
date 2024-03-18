import pandas as pd
import numpy as np
import json
import uuid
from functools import reduce
from psycopg2.errors import ForeignKeyViolation
from sqlalchemy import create_engine

from shared_modules.utils import (
    get_subquestions,
    connection,
    md5_encode,
    parse_json_config,
    query_bronze_layer,
    title_str,
    encode,
    query_db,
    replace_bad_characters,
)
from shared_modules.env_utils import CONFIGS, CSV_PATH, get_engine_str


def get_custom_forms(conn, df):
    cur = conn.cursor()

    fk_missing_rows = []
    missing_qa_rows = []

    # df["fields"] = df["fields"].apply(json.loads)
    # exploded_df = df.explode("fields")

    # exploded_df["fields"] = exploded_df["fields"].apply(lambda x: get_subquestions(x))
    # exploded_df = exploded_df.explode("fields")

    # exploded_df["title"] = exploded_df["fields"].apply(lambda x: x.get("title"))
    # exploded_df["question_answer"] = exploded_df["fields"].apply(
    #     lambda x: x.get("answer")
    # )

    cols_to_check = [
        "surveyingUser",
        "communityname",
        "question_answer"
    ]
    missing_ind_dict = {col: df[col].notnull() for col in cols_to_check}
    missing_rows_dict = {col: df[~idx] for col, idx in missing_ind_dict.items()}

    conditions = list(missing_ind_dict.values())

    # only not na in all check columns
    combined_condition = reduce(lambda x, y: x & y, conditions)

    # Filter the DataFrame based on the combined condition
    exploded_df = df[combined_condition].reset_index(drop=True)

    missing_dict = {"hhids": [], "comms": [], "answers": [], "users": []}

    insert_count = 0
    ignore_count = 0
    check_count = 0

    title_cols =[
        "surveyingOrganization",
        "communityname",
        "surveyingUser"
    ]
    for col in title_cols:
        exploded_df[col] = exploded_df[col].apply(lambda x: title_str(x))

    inserted_uuids = [] 
    existing_qs = list(query_db("SELECT DISTINCT question FROM question_dim")["question"].unique())

    existing_patients = list(query_db("SELECT DISTINCT  FROM question_dim")["question"].unique())

    print(exploded_df.shape)
    exploded_df = exploded_df[exploded_df["title"].isin(existing_qs)]
    print("post qs")
    print(exploded_df.shape)


    exploded_df["form_id"] = exploded_df["formSpecificationsId"].apply(lambda x: md5_encode(x))

    #print(exploded_df["form_id"].unique())

    existing_forms = list(query_db("SELECT DISTINCT uuid FROM form_dim")["uuid"].unique())
    #print(existing_forms)
    
    exploded_df = exploded_df[exploded_df["form_id"].isin(existing_forms)]
    print("final exploded shape")
    print(exploded_df.shape)

    print(len(exploded_df["title"].unique()))

    error_dict = {}


    for i, row in exploded_df.iterrows():
        object_id = row.get("client.objectId")
        user = row.get("surveyingUser")
        survey_org = row.get("surveyingOrganization")
        form = row.get("formSpecificationsId")
        created_at = row.get("createdAt")
        updated_at = row.get("updatedAt")
        household = row.get("household")
        community_name = row.get("communityname")
        title = row.get("title")
        question_answer = row.get("question_answer")

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
                check = ("test" in field.lower()) or ("forgot" in field.lower())
                check_list.append(check)
        if any(check_list):
            check_count += 1
            continue

        if household in [None, np.nan]:
            household_id = None
        else:
            household_id = md5_encode(household)

        patient_id = md5_encode(object_id)
        user_id = md5_encode(user)
        surveying_organization_id = md5_encode(survey_org)
        form_id = md5_encode(form)
        community_id = md5_encode(community_name)

        # if form_id not in existing_forms:
        #     continue

        ignore_questions = [
            'surveyinguser',
            'surveyingorganization',
            'phoneos',
            'appversion',
        ]

        if title.lower().strip() in ignore_questions:
            ignore_count += 1
            #print(title)
            continue

        #title = replace_bad_characters(title)

        question_id = md5_encode(title)

        #print("title")
        #print(title)
        #print(question_id)

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

            insert_count += 1

        except ForeignKeyViolation as e:
            insert_tuple = tuple(
                list(insert_tuple)[:5] + [title, object_id] + list(insert_tuple[5:])
            )
            fk_missing_rows.append(insert_tuple)
            if "question_dim" in str(e):
                error_table = "question_dim"
            elif "patient_dim" in str(e):
                error_table = "patient_dim"
            else:
                error_table = "other"

            if error_table not in error_dict.keys():
                error_dict[error_table] = 1
            else:
                error_dict[error_table] += 1
            cur.execute("ROLLBACK")
            continue

    conn.commit()

    # Close the database connection and cursor
    cur.close()

    # NOTE from 2024-03-11
    # getting a ton of FK errors, so nothing is getting inserted
    # first one I see is patient dim error
    print("custom form insert count")
    print(insert_count)
    print("check count")
    print(check_count)
    print("ignore count")
    print(ignore_count)
    print("error dict")
    print(error_dict)

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

    for table, missing_df in missing_rows_dict.items():
        #missing_df = pd.DataFrame.from_records(missing, columns=cols)
        if missing_df.shape[0] > 0:
            missing_df.to_csv(f"{CSV_PATH}/customforms_missing_{table}.csv", index=False)

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


def add_nosql_to_fact(con, table_name, survey_df):
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
    merged.to_csv(F"{CSV_PATH}/merged_{table_name}.csv", index=False)
    comb_df = merged[id_cols + questions].melt(
        id_vars=id_cols, var_name="question", value_name="answer"
    )

    ignore_questions = ["searchIndex", "surveyingUser"] + [col for col in questions if "location" in col]
    comb_df = comb_df[~comb_df['question'].isin(ignore_questions)]

    comb_df.to_csv(f"{CSV_PATH}/comb_df_{table_name}.csv")

    fk_missing_rows = []
    notnull_missing_rows = []
    user_fk = []
    patient_fk = []

    missing_dict = {"hhids": [], "users": [], "comms": [], "answers": []}

    insert_count = 0
    fk_count = 0
    user_fk_count = 0
    test_check_count = 0
    ignore_questions_count = 0
    patient_fk_count = 0

    cols_to_check = [
        "surveyingUser",
        "communityname",
        "answer"
    ]
    missing_ind_dict = {col: comb_df[col].notnull() for col in cols_to_check}
    missing_rows_dict = {col: comb_df[~idx] for col, idx in missing_ind_dict.items()}

    conditions = list(missing_ind_dict.values())

    # only not na in all check columns
    combined_condition = reduce(lambda x, y: x & y, conditions)

    # Filter the DataFrame based on the combined condition

    comb_df = comb_df[combined_condition].reset_index(drop=True)

    title_cols =[
        "surveyingOrganization",
        "communityname",
        "surveyingUser"
    ]
    for col in title_cols:
        comb_df[col] = comb_df[col].apply(lambda x: title_str(x))

    # TODO: use to_sql instead of insert 
    # will have to use pd.apply to get all the ids and do all checks
    # encode_cols = {
    #     "surveying_organization_id": "surveyingOrganization",
    #     "household_id": "householdId",
    #     "patient_id": "objectId",
    #     "community_id": "communityname",
    #     "question_id": "question",
    #     "user_id": "surveyingUser"
    # }

    # for new_name, col in encode_cols.items():
    #     comb_df[new_name] = comb_df[col].apply(lambda x: encode(x))

    # comb_df['form_id'] = encode(table_name)
    # def generate_uuid():
    #     return str(uuid.uuid4())
    # comb_df['uuid'] = comb_df.apply(generate_uuid, axis=1)
    # comb_df['question_answer'] = comb_df['answer']

    # final_cols = [
    #     "uuid",
    #     "surveying_organization_id",
    #     "surveying_user_id",
    #     "community_id",
    #     "question_id",
    #     "question_answer",
    #     "created_at",
    #     "updated_at",
    #     "patient_id",
    #     "household_id",
    #     "form_id"
    # ]

    # comb_df = comb_df[final_cols]

    # engine_str = get_engine_str()

    # engine = create_engine(engine_str)

    # # Write the DataFrame to PostgreSQL
    # comb_df.to_sql('survey_fact', engine, index=False, if_exists='append')

    # # Close the database connection
    # engine.dispose()
    for i, row in comb_df.iterrows():
        created_at = row["createdAt"]
        updated_at = row["updatedAt"]
        question_name = row["question"]
        question_answer = row["answer"]

        object_id = row["objectId"]
        survey_org = row["surveyingOrganization"]
        user = row["surveyingUser"]
        community_name = row["communityname"]
        nosql_household_id = row["householdId"]

        question_name = replace_bad_characters(question_name)
       
        check_list = []
        for field in [nosql_household_id, user, community_name]:
            if isinstance(field, str):
                check = ("test" in field.lower()) or ("forgot" in field.lower()) or ("experimental" in field.lower())
                check_list.append(check)
        if any(check_list):
            test_check_count += 1
            continue

        if question_name.lower().strip() in ['surveyinguser']:
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

            insert_count += 1

        except ForeignKeyViolation as e:
            if "surveying_user_id" in str(e):
                user_fk.append(insert_tuple)
                user_fk_count += 1
            elif "patient_id" in str(e):
                patient_fk.append(insert_tuple)
                patient_fk_count += 1
            else:
                # Handle other integrity errors if needed
                fk_missing_rows.append(insert_tuple)
                fk_count += 1
            cur.execute("ROLLBACK")
            continue

    con.commit()

    # Close the database connection and cursor
    cur.close()

    total_missing = sum(len(lst) for lst in missing_dict.values())

    print("comb size")
    print(comb_df.shape)
    print("nosql insert count")
    print(insert_count)
    print("nosql fk count")
    print(fk_count)
    print("user fk")
    print(user_fk_count)
    print("patient fk")
    print(patient_fk_count)
    print("test check count")
    print(test_check_count)
    print("ignore questions count")
    print(ignore_questions_count)
    for name, lst in missing_dict.items():
        print(f"{name} count")
        print(len(lst))
    print("total missing")
    print(total_missing)

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

    for table, missing_df in missing_rows_dict.items():
        #missing_df = pd.DataFrame.from_records(missing, columns=cols)
        if missing_df.shape[0] > 0:
            missing_df.to_csv(
                f"{CSV_PATH}/add_nosql_to_fact_{table_name}_missing_{table}.csv", index=False
            )

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
