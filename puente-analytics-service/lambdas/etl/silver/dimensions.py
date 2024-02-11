from shared_modules.utils import (
    connection,
    unique_combos,
    coalesce_pkey,
    md5_encode,
    add_surveyuser_column,
    parse_json_config,
    title_str,
    unique_values,
    query_db,
    replace_bad_characters,
)
from shared_modules.env_utils import CONFIGS, CSV_PATH

import pandas as pd
import numpy as np
import json
import datetime
from psycopg2.errors import ForeignKeyViolation

"""
The following is a huge series of creating all the dimension tables
Kinda repetitive, I'm sure there's a way to better generalize but this works
And keeps it clear what is happening in all the tables
"""


def get_community_dim(df):
    con = connection()
    cur = con.cursor()
    df['communityname'] = df['communityname'].apply(lambda x: title_str(x))
    communities = unique_combos(df, ["communityname", "city", "region"])
    communities = coalesce_pkey(communities, "communityname")
    now = datetime.datetime.utcnow()
    for i, community_row in communities.iterrows():
        community = community_row.get("communityname")
        city = community_row.get("city")
        region = community_row.get("region")

        uuid = md5_encode(community)

        cur.execute(
            f"""
                INSERT INTO community_dim (uuid, name, city, region, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
            (uuid, community, city, region, now, now),
        )

    # Commit the changes to the database
    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"communities": communities.to_json()}),
        "isBase64Encoded": False,
    }


def get_form_dim(df):
    # this comes from formspecificationsv2
    con = connection()
    cur = con.cursor()
    forms = unique_combos(
        df, ["objectId", "name", "description", "customForm", "createdAt", "updatedAt"]
    )
    forms = coalesce_pkey(forms, "objectId")
    now = datetime.datetime.utcnow()
    for i, form_row in forms.iterrows():
        form = form_row.get("objectId")
        name = form_row.get("name")
        description = form_row.get("description")
        is_custom_form = form_row.get("customForm")
        created_at = form_row.get("createdAt", now)
        updated_at = form_row.get("updatedAt", now)
        uuid = md5_encode(form)
        cur.execute(
            f"""
                INSERT INTO form_dim (uuid, name, description, is_custom_form, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
            (uuid, name, description, is_custom_form, created_at, updated_at),
        )

    # Commit the changes to the database
    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"forms": forms.to_json()}),
        "isBase64Encoded": False,
    }


def get_surveying_organization_dim(df):
    con = connection()
    cur = con.cursor()
    df['surveyingOrganization'] = df['surveyingOrganization'].apply(lambda x: title_str(x))
    survey_orgs = df["surveyingOrganization"].unique()
    now = datetime.datetime.utcnow()
    for survey_org in survey_orgs:
        uuid = md5_encode(survey_org)
        cur.execute(
            f"""
                INSERT INTO surveying_organization_dim (uuid, name, created_at, updated_at)
                VALUES (%s, %s, %s, %s)
                """,
            (uuid, survey_org, now, now),
        )

    # Commit the changes to the database
    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"surveying_organizations": survey_orgs.tolist()}),
        "isBase64Encoded": False,
    }


def get_users_dim(df):
    con = connection()
    cur = con.cursor()
    users = unique_combos(
        df,
        [
            "objectId",
            "username",
            "firstname",
            "lastname",
            "phonenumber",
            "role",
            "createdAt",
            "updatedAt",
            "organization",
        ],
    )
    users = coalesce_pkey(users, "objectId")
    users = add_surveyuser_column(users)
    grouped = users.groupby("survey_user").nunique().reset_index()
    dups = grouped.loc[grouped["objectId"] > 1]
    if dups.shape[0] > 0:
        dups.to_csv(f"{CSV_PATH}/all_duplicates.csv", index=False)
    missing_names = []
    missing_surveyorgs = []
    for i, user_row in users.iterrows():
        survey_user = user_row.get("survey_user")
        if survey_user in dups["survey_user"].values:
            continue
        survey_org = user_row.get("organization")
        user_name = user_row.get("user_name")
        first_name = user_row.get("firstname")
        last_name = user_row.get("lastname")
        phone_number = user_row.get("phonenumber")
        role = user_row.get("role")
        created_at = user_row.get("createdAt")
        updated_at = user_row.get("updatedAt")

        check_list = []
        for field in [survey_org, first_name, last_name]:
            if isinstance(field, str):
                check = "test" in field.lower()
                check_list.append(check)
        if any(check_list):
            continue

        uuid = md5_encode(title_str(survey_user))
        if survey_org is None:
            missing_surveyorgs.append(
                (
                    uuid,
                    survey_user,
                    user_name,
                    first_name,
                    last_name,
                    created_at,
                    updated_at,
                    phone_number,
                    role,
                    survey_org,
                    None,
                )
            )
            continue
        survey_org_id = md5_encode(title_str(survey_org))
        if first_name is None or last_name is None:
            missing_names.append(
                (
                    uuid,
                    survey_user,
                    user_name,
                    first_name,
                    last_name,
                    created_at,
                    updated_at,
                    phone_number,
                    role,
                    survey_org,
                    survey_org_id,
                )
            )
            continue
        try:
            cur.execute(
                f"""
                    INSERT INTO users_dim (uuid, survey_user, user_name, first_name, last_name, created_at, updated_at, phone_number, role, surveying_organization_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                (
                    uuid,
                    survey_user,
                    user_name,
                    first_name,
                    last_name,
                    created_at,
                    updated_at,
                    phone_number,
                    role,
                    survey_org_id,
                ),
            )
        except:
            missing_surveyorgs.append((
                    uuid,
                    survey_user,
                    user_name,
                    first_name,
                    last_name,
                    created_at,
                    updated_at,
                    phone_number,
                    role,
                    survey_org,
                    None,
                ))
            cur.execute("ROLLBACK")
            continue

    # Commit the changes to the database
    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()

    cols = [
        "uuid",
        "survey_user",
        "user_name",
        "first_name",
        "last_name",
        "created_at",
        "updated_at",
        "phone_number",
        "role",
        "survey_org",
        "survey_org_id",
    ]
    missing_names_df = pd.DataFrame.from_records(missing_names, columns=cols)
    if missing_names_df.shape[0] > 0:
        missing_names_df.to_csv(f"{CSV_PATH}/missing_surveyorgs_usersdim.csv", index=False)

    missing_surveyorgs_df = pd.DataFrame.from_records(missing_surveyorgs, columns=cols)
    if missing_surveyorgs_df.shape[0] > 0:
        missing_surveyorgs_df.to_csv(f"{CSV_PATH}/missing_surveyorgs_usersdim.csv", index=False)

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"users": users.to_json()}),
        "isBase64Encoded": False,
    }


def get_household_dim(df):
    con = connection()
    cur = con.cursor()
    households = unique_combos(
        df, ["householdId", "latitude", "longitude", "communityname"]
    )
    households = coalesce_pkey(households, "householdId")
    missing_comms = []
    missing_hhid = []
    now = datetime.datetime.utcnow()
    for i, household_row in households.iterrows():
        household_id = household_row.get("householdId")
        community_name = household_row.get("communityname")
        lat = household_row.get("latitude")
        lon = household_row.get("longitude")
        check_list = []
        for field in [community_name, household_id]:
            if isinstance(field, str):
                check = "test" in field.lower()
                check_list.append(check)
        if any(check_list):
            continue

        if community_name is None:
            missing_comms.append((household_id, community_name, lat, lon))
            continue
        if household_id is None:
            missing_hhid.append((household_id, community_name, lat, lon))
            continue
        uuid = md5_encode(household_id)
        community = md5_encode(community_name)
        cur.execute(
            f"""
                INSERT INTO household_dim (uuid, latitude, longitude, created_at, updated_at, community_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
            (uuid, lat, lon, now, now, community),
        )

    # Commit the changes to the database
    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()

    cols = ["household_id", "community_name", "lat", "lon"]
    missing_comms_df = pd.DataFrame.from_records(missing_comms, columns=cols)
    if missing_comms_df.shape[0] > 0:
        missing_comms_df.to_csv(f"{CSV_PATH}/missing_comms_householddim.csv", index=False)
    missing_hhid_df = pd.DataFrame.from_records(missing_hhid, columns=cols)
    if missing_hhid_df.shape[0] > 0:
        missing_hhid_df.to_csv(f"{CSV_PATH}/missing_hhid_householddim.csv", index=False)

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"households": households.to_json()}),
        "isBase64Encoded": False,
    }


def get_patient_dim(df):
    # this data comes from surveyfact
    con = connection()
    cur = con.cursor()
    df["age"] = df["age"].replace({"nan": None, "": None, " ": None, np.nan: None})
    patients = unique_combos(
        df,
        [
            "objectId",
            "fname",
            "lname",
            "sex",
            "age",
            "nickname",
            "telephoneNumber",
            "householdId",
        ],
    )
    patients = coalesce_pkey(patients, "objectId")

    missing_rows = []
    missing_hhid = []
    missing_names = []
    now = datetime.datetime.utcnow()
    for i, patient_row in patients.iterrows():
        patient_id = patient_row.get("objectId")
        household_id = patient_row.get("householdId")
        first_name = patient_row.get("fname")
        last_name = patient_row.get("lname")
        nick_name = patient_row.get("nickname")
        sex = patient_row.get("sex")
        age = patient_row.get("age")
        if age in ["11 meses"]:
            age = "0"
        if age not in [np.nan, None]:
            chars_to_remove = ["-", ",", "."]
            for char in chars_to_remove:
                age = age.replace(char, "")

        phone_number = patient_row.get("telephoneNumber")

        check_list = []
        for field in [first_name, last_name, household_id]:
            if isinstance(field, str):
                check = "test" in field.lower()
                check_list.append(check)
        if any(check_list):
            continue

        if household_id is None:
            household_uuid = None
        else:
            household_uuid = md5_encode(household_id)

        if (first_name in [np.nan, None]) | (last_name in [np.nan, None]):
            missing_names.append((first_name, last_name, patient_id, household_id))
            continue

        uuid = md5_encode(patient_id)

        try:
            cur.execute(
                f"""
                    INSERT INTO patient_dim (uuid, first_name, last_name, nick_name, sex, age, created_at, updated_at, phone_number, household_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                (
                    uuid,
                    first_name,
                    last_name,
                    nick_name,
                    sex,
                    age,
                    now,
                    now,
                    phone_number,
                    household_uuid,
                ),
            )
        except ForeignKeyViolation:
            missing_rows.append((patient_id, household_id, household_uuid))
            cur.execute("ROLLBACK")
            continue

    cols = [
        "uuid",
        "first_name",
        "last_name",
        "nick_name",
        "sex",
        "age",
        "phone_number",
        "household_id",
        "household_uuid",
    ]

    missing_hhid_df = pd.DataFrame.from_records(missing_hhid, columns=cols)
    if missing_hhid_df.shape[0] > 0:
        missing_hhid_df.to_csv(f"{CSV_PATH}/missing_hhid_patient.csv", index=False)

    cols = ["patient_id", "household_id", "household_uuid"]
    missing_rows_df = pd.DataFrame.from_records(missing_rows, columns=cols)
    if missing_rows_df.shape[0] > 0:
        missing_rows_df.to_csv(f"{CSV_PATH}/missing_patient_id_patient_dim.csv", index=False)

    cols = ["first_name", "last_name", "patient_id", "household_id"]
    missing_names_df = pd.DataFrame.from_records(missing_names, columns=cols)
    if missing_names_df.shape[0] > 0:
        missing_names_df.to_csv(f"{CSV_PATH}./missing_names_patient.csv", index=False)

    # Commit the changes to the database
    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"patients": patients.to_json()}),
        "isBase64Encoded": False,
    }


def get_question_dim(df):
    # this comes from formspecificationsv2
    con = connection()
    cur = con.cursor()

    # grouping by fields returns nan for some reason??
    unique_forms = unique_combos(df, ["objectId", "createdAt", "updatedAt"])
    unique_forms = coalesce_pkey(unique_forms, "objectId")
    forms = unique_forms.merge(
        df[["objectId", "fields", "createdAt", "updatedAt"]],
        on=["objectId", "createdAt", "updatedAt"],
    )
    inserted_uuids = []
    missing_ids = []
    missing_labels = []
    for i, form_row in forms.iterrows():
        form = form_row.get("objectId")
        form_created_at = form_row.get("createdAt")
        form_updated_at = form_row.get("updatedAt")
        form_id = md5_encode(form)

        question_list = json.loads(form_row.get("fields"))

        for question in question_list:
            id = question.get("id")
            field_type = question.get("fieldType")
            formik_key = question.get("formikKey")
            question_label = question.get("label")
            # note sure the best way to handle this
            if field_type in ["select", "selectMulti"]:
                options = question.get("options")
                options_list = [option["label"] for option in options]
            else:
                options_list = None

            check_list = []
            for field in [
                id,
                question_label,
            ]:
                if isinstance(field, str):
                    check = "test" in field.lower()
                    check_list.append(check)
            if any(check_list):
                continue

            if question_label in ["geolocation", "surveyingUser"]:
                continue

            if id is None:
                missing_ids.append(
                    (
                        id,
                        field_type,
                        formik_key,
                        question_label,
                        options_list,
                        form,
                        form_id,
                    )
                )
                continue
            if question_label is None:
                missing_labels.append(
                    (
                        id,
                        field_type,
                        formik_key,
                        question_label,
                        options_list,
                        form,
                        form_id,
                    )
                )
                continue

            question_label = replace_bad_characters(question_label)
            uuid = md5_encode(id)

            if uuid in inserted_uuids:
                continue
            else:
                cur.execute(
                    f"""
                    INSERT INTO question_dim (uuid, question, field_type, formik_key, options, created_at, updated_at, form_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        uuid,
                        question_label,
                        field_type,
                        formik_key,
                        options_list,
                        form_created_at,
                        form_updated_at,
                        form_id,
                    ),
                )
                inserted_uuids.append(uuid)

    # Commit the changes to the database
    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()

    cols = [
        "id",
        "field_type",
        "formik_key",
        "question_label",
        "options_list",
        "form",
        "form_id",
    ]
    missing_ids_df = pd.DataFrame.from_records(missing_ids, columns=cols)
    if missing_ids_df.shape[0] > 0:
        missing_ids_df.to_csv(f"{CSV_PATH}/missing_ids_question.csv", index=False)
    missing_labels_df = pd.DataFrame.from_records(missing_labels, columns=cols)
    if missing_labels_df.shape[0] > 0:
        missing_labels_df.to_csv(f"{CSV_PATH}/missing_labels_question.csv", index=False)

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"questions": question_list}),
        "isBase64Encoded": False,
    }


def add_nosql_to_forms(name, description, now):
    uuid = md5_encode(name)

    con = connection()
    cur = con.cursor()
    cur.execute(
        f"""
                INSERT INTO form_dim (uuid, name, description, is_custom_form, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
        (uuid, name, description, False, now, now),
    )

    # Commit the changes to the database
    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()


def ingest_nosql_configs(configs):
    con = connection()
    cur = con.cursor()
    for table_name, config_path in configs.items():
        form_id = md5_encode(table_name)
        to_insert = parse_json_config(config_path)
        now = datetime.datetime.now()
        for question in to_insert:
            uuid, question, field_type, formik_key, options = question
            cur.execute(
                f"""
                INSERT INTO question_dim (uuid, question, field_type, formik_key, options, created_at, updated_at, form_id)
                VALUES ({uuid}, {question}, {field_type}, {formik_key}, {options}, {now}, {now}, {form_id})
                """
            )

    # Commit the changes to the database
    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()


def ingest_nosql_table_questions(table_name):
    config = parse_json_config(CONFIGS[table_name])

    con = connection()
    cur = con.cursor()

    now = datetime.datetime.now()

    form_id = md5_encode(table_name)

    for question_tuple in config:
        uuid, label, formik_key, field_type, options = question_tuple  #
        label = replace_bad_characters(label)
        if label in ["surveyingUser"]:
            continue
        uuid = md5_encode(formik_key)
        cur.execute(
            f"""
                INSERT INTO question_dim (uuid, question, field_type, formik_key, options, created_at, updated_at, form_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
            (uuid, label, field_type, formik_key, options, now, now, form_id),
        )

        # Commit the changes to the database
    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"questions": config}),
        "isBase64Encoded": False,
    }

def get_custom_form_questions(form_results):
    con = connection()
    cur = con.cursor()

    #form_results = form_results[~form_results["formSpecificationsId"].isin(inactive_forms)]

    ignore_questions = [
        "surveyingUser",
        "surveyingOrganization",
        "appVersion",
        "phoneOS"
    ]

    print("1")
    print(form_results[form_results['title']=='Nombre de Medicamento'])

    options_fr = form_results[~form_results["title"].isin(ignore_questions)]
    options = options_fr.groupby(["title"])["question_answer"].agg(lambda x: unique_values(x)).reset_index().rename({"question_answer": "options"}, axis=1)
    options["num_answers"] = options["options"].apply(len)

    options_fr = options_fr.merge(options, on="title", how="left")

    print("2")
    print(options_fr[options_fr['title']=='Nombre de Medicamento'])

    options_fr["field_type"] = None
    options_fr["is_list"] = options_fr["question_answer"].apply(lambda x: isinstance(x, list))

    options_fr.loc[options_fr["num_answers"] == 1, "field_type"] = "input"
    options_fr.loc[options_fr["num_answers"] > 1, "field_type"] = "select"
    options_fr.loc[options_fr["is_list"], "field_type"] = "selectMulti"

    options_fr["form_id"] = options_fr["formSpecificationsId"].apply(lambda x: md5_encode(x))

    existing_forms = list(query_db("SELECT DISTINCT uuid FROM form_dim")["uuid"].unique())
    options_fr = options_fr[options_fr["form_id"].isin(existing_forms)]

    print("3")
    print(options_fr[options_fr['title']=='Nombre de Medicamento'])

    inserted_uuids = [] 
    existing_qs = list(query_db("SELECT DISTINCT question FROM question_dim")["question"].unique())

    options_fr = options_fr[~options_fr["title"].isin(existing_qs)]
    options_fr = coalesce_pkey(options_fr, "title")

    print("4")
    print(options_fr[options_fr['title']=='Nombre de Medicamento'])

    for i, row in options_fr.iterrows():
        form = row.get("formSpecificationsId")
        form_created_at = row.get("createdAt")
        form_updated_at = row.get("updatedAt")
        question = row.get("title")
        options_list = row.get("options")
        field_type = row.get("field_type")
        # TODO: come up with a way of defining this
        formik_key = None

        uuid = md5_encode(question)
        form_id = md5_encode(form)

        if uuid in inserted_uuids:
            continue
        else:
            cur.execute(
                f"""
                INSERT INTO question_dim (uuid, question, field_type, formik_key, options, created_at, updated_at, form_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    uuid,
                    question,
                    field_type,
                    formik_key,
                    options_list,
                    form_created_at,
                    form_updated_at,
                    form_id,
                ),
            )
            inserted_uuids.append(uuid)

        # Commit the changes to the database
        con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()


