import os
import psycopg2
import datetime
import json
import numpy as np
import pandas as pd
import uuid

from psycopg2.errors import ForeignKeyViolation, NotNullViolation

#with open("survey_data_config.json", 'r') as j:
#     contents = json.loads(j.read())
#     print(contents)
#print(os.getcwd())
#survey_config = json.loads("./survey_data_config.json")
#env_config = json.loads('env_data_config.json')

import hashlib

#survey_config

import json
import os
import sys; sys.path.append(os.path.join(os.path.dirname(__file__)))

import requests
from pandas import json_normalize

def add_surveyuser_column(df):
    df['default_value'] = df['firstname'] + df['lastname']
    df['survey_user'] = df[['default_value', 'username', 'firstname', 'lastname']].bfill(axis=1).iloc[:, 0]
    return df

def restCall(
    specifier, custom_form_id, url="https://parseapi.back4app.com/classes/" #url="https://puente.back4app.io/classes/" #
):
    """
    Parameters
    ----------
    specifier: string, required
        Parse model to query

    custom_form_id: string, not required
        Custom form object id

    url: string, required
        Parse endpoint
    Returns
    ------
    Pandas dataframe with query results

    """

    #
    # Build Request URL, Headers, and Parameters
    #

    common_params = {
        "order": "-updatedAt",
        "limit": 500000,
    }
    if specifier == 'users':
        split_url = url.split("/")
        url = "/".join(split_url[:len(split_url)-2]) + "/"
        #print('user url')
        #print(url)
    combined_url = url + specifier

    headers = {
        "Content-Type": "application/json",
        "X-Parse-Application-Id": APP_ID,
        "X-Parse-Master-Key": MASTER_KEY,
        "X-Parse-REST-API-Key": REST_API_KEY,
    }

    

    params = dict()

    if specifier != "FormResults" and specifier != "FormAssetResults":
        params = {
            **common_params,
        }
    else:
        # custom forms need to ensure that the correct custom form results are returned
        params = {
            **common_params,
            "where": {json.dumps({
                "formSpecificationsId": {"$in": [custom_form_id]}}
            )}
        }

    #
    # MAKE REST API CALL
    #
    response = requests.get(
        combined_url,
        params=params,
        headers=headers
    )
    response.raise_for_status()

    json_obj = response.json()
    #print('json_obj')
    #print(json_obj)
    # normalize (ie flatten) data, turns it into a pandas df
    normalized = json_normalize(json_obj["results"])

    #string_cols = normalized.select_dtypes(include=[object]).columns
    #normalized[string_cols] = normalized[string_cols].apply(lambda x: x.str.strip())

    #normalized.to_csv(os.getcwd()+'/test.csv')

    # # join non surveyData forms to surveyData
    # if specifier != "SurveyData" and specifier != "Assets":
    #     combined_url = (
    #         url + "SurveyData" if specifier != "FormAssetResults" else url + "Assets"
    #     )
    #     params = {
    #         **common_params,
    #     }
    #     response_primary = requests.get(combined_url, params=params, headers=headers)
    #     json_obj_primary = response_primary.json()
    #     normalized_primary = json_normalize(json_obj_primary["results"])
    #     normalized = normalized.rename(
    #         columns={
    #             "objectId": "objectIdSupplementary",
    #             "client.objectId": "objectId",
    #             "surveyingUser": "surveyingUserSupplementary",
    #             "surveyingOrganization": "surveyingOrganizationSupplementary",
    #             "createdAt": "createdAtSupplementary",
    #         }
    #     )

    #     return normalized_primary, normalized

    return normalized

def to_camel_case(text):
    s = text.replace("-", " ").replace("_", " ")
    s = s.split()
    if len(text) == 0:
        return text
    return s[0] + ''.join(i.capitalize() for i in s[1:])

def md5_encode(s):
    return hashlib.md5(s.encode('utf-8')).hexdigest()

def parse_json_config(json_path):
    with open(json_path, 'r') as j:
        questions = json.loads(j.read())
        question_values_list = []
        for question in questions:
            formikkey = question['formikKey']
            label = question['label']
            uuid = md5_encode(formikkey)
            field_type = question['fieldType']
            if 'options' in question.keys():
                options = [option.get('value') for option in question['options']]
            else:
                options = None

            question_values = (uuid, label, formikkey, field_type, options)
            question_values_list.append(question_values)

    return question_values_list

with open("../env.json") as file:
    env = json.load(file)["AnalyticsLambdaFunctionETL"]

PG_HOST = env.get('PG_HOST')
PG_PORT = env.get('PG_PORT')
PG_DATABASE = env.get('PG_DATABASE')
PG_USERNAME = env.get('PG_USERNAME')
PG_PASSWORD = env.get('PG_PASSWORD')
APP_ID = env.get('APP_ID')
REST_API_KEY = env.get('REST_API_KEY')
MASTER_KEY = env.get('MASTER_KEY')

NOSQL_TABLES = {
    'HistoryEnvironmentalHealth': 'Marketplace form for questions related to patients environment',
    'EvaluationMedical': 'Marketplace form for questions related to patients current medical health',
    'Vitals': "Marketplace form for questions related to patients' vitals"
}

CONFIGS = {
    "HistoryEnvironmentalHealth": "env_health_config.json",
    "EvaluationMedical": "eval_medical_config.json",
    "Vitals": "vitals_config.json"
}

def unique_combos(df, col_list):
    string_dtypes = df.select_dtypes(include=[object]).columns.tolist()
    strings_in_col_list = [col for col in col_list if col in string_dtypes]
    df[strings_in_col_list] = df[strings_in_col_list].apply(lambda x: x.str.strip())
    unique_combos = df.groupby(col_list, dropna=False).size().reset_index()[col_list]
    return unique_combos

def coalesce_pkey(val_df, pkey):
    return val_df.groupby(pkey).first().reset_index()

def connection():
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        port=PG_PORT,
        user=PG_USERNAME,
        password=PG_PASSWORD
    )
    return conn


def drop_tables():
    #be very sure about running this lol
    conn = connection()
    cur = conn.cursor()
    tables = ['surveying_organization_dim', 'users_dim', 'community_dim', 'household_dim', 'patient_dim', "question_dim", 'form_dim', 'survey_fact']
    for table in tables:
       cur.execute(f"DROP TABLE {table} CASCADE")
       conn.commit()

    cur.close()
    conn.close()

#drop_tables()

# cur.execute("select * from information_schema.tables where table_schema = 'public'")
# mobile_records = cur.fetchall()
# print(mobile_records)

def initialize_tables():
    conn = connection()
    cur = conn.cursor()
    users_q = f"""
    CREATE TABLE IF NOT EXISTS users_dim (
        uuid UUID PRIMARY KEY,
        survey_user VARCHAR(255) NOT NULL,
        user_name VARCHAR(255) NOT NULL,
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        phone_number VARCHAR(255),
        role VARCHAR(255),
        surveying_organization_id UUID NOT NULL REFERENCES surveying_organization_dim (uuid)
    );
    """

    surveying_org_q = f"""
    CREATE TABLE surveying_organization_dim (
        uuid UUID PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """

    community_q = f"""
    CREATE TABLE community_dim (
        uuid UUID PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        city VARCHAR(255),
        region VARCHAR(255),
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """

    household_q = f"""
    CREATE TABLE household_dim (
        uuid UUID PRIMARY KEY,
        latitude NUMERIC(9,6) NOT NULL,
        longitude NUMERIC(9,6) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        community_id UUID NOT NULL REFERENCES community_dim (uuid)
    );
    """

    patient_q = f"""
    CREATE TABLE patient_dim (
        uuid UUID PRIMARY KEY,
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        nick_name VARCHAR(255),
        sex VARCHAR(255),
        age INT, 
        phone_number VARCHAR(255),
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        household_id UUID NOT NULL REFERENCES household_dim (uuid)
    )
    """

    form_q = f"""
    CREATE TABLE form_dim (
        uuid UUID PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        description VARCHAR(10000),
        is_custom_form BOOLEAN NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """

    question_q = f"""
    CREATE TABLE question_dim (
        uuid UUID PRIMARY KEY,
        question VARCHAR(255) NOT NULL,
        field_type VARCHAR(255) NOT NULL,
        formik_key VARCHAR(255),
        options TEXT[],
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        form_id UUID NOT NULL REFERENCES form_dim (uuid)
    );
    """

    #surveying_user_id UUID NOT NULL REFERENCES users_dim (uuid),
    #surveying_user_id VARCHAR(1000),
    survey_fact_q = f"""
    CREATE TABLE survey_fact (
        uuid UUID PRIMARY KEY,
        surveying_organization_id UUID NOT NULL REFERENCES surveying_organization_dim (uuid),
        surveying_user_id UUID NOT NULL REFERENCES users_dim (uuid),
        community_id UUID NOT NULL REFERENCES community_dim (uuid),
        question_id UUID NOT NULL REFERENCES question_dim (uuid),
        question_answer VARCHAR(255) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        form_id UUID NOT NULL REFERENCES form_dim (uuid),
        patient_id UUID NOT NULL REFERENCES patient_dim (uuid),
        household_id UUID NOT NULL REFERENCES household_dim (uuid)
    );
    """

    create_qs = [surveying_org_q, users_q, community_q, household_q, patient_q, form_q, question_q, survey_fact_q]
    q_names = ['survey_org', 'users', 'comm', 'house', 'patient', 'form', 'question', 'survey_fact']
    for q, name in zip(create_qs, q_names):
        #print('query:')
        #print(name)
        cur.execute(q)
        # Commit the changes to the database
        conn.commit()

    # Close the database connection and cursor
    cur.close()
    conn.close()


'''
The following is a huge series of creating all the dimension/fact tables
Kinda repetitive, I'm sure there's a way to better generalize but this works
And keeps it clear what is happening in all the tables
'''

def get_community_dim(df):
    con = connection()
    cur = con.cursor()
    communities = unique_combos(df, ['communityname', 'city', 'region'])
    communities = coalesce_pkey(communities, 'communityname')
    now = datetime.datetime.utcnow()
    for i, community_row in communities.iterrows():
        #'community')
        #print(community_row)
        community = community_row.get('communityname')
        city = community_row.get('city')
        region = community_row.get('region')
    
        if (community in [np.nan, None, "", " "])|(isinstance(community, float)):
           # print('continuing')
            #log the communities that have problems here
            continue

        uuid = md5_encode(community)

        cur.execute(
                f"""
                INSERT INTO community_dim (uuid, name, city, region, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (uuid, community, city, region, now, now)
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
    #this comes from formspecificationsv2
    con = connection()
    cur = con.cursor()
    forms = unique_combos(df, ['objectId', 'name', 'description', 'customForm', 'createdAt', 'updatedAt'])
    forms = coalesce_pkey(forms, 'objectId')
    now = datetime.datetime.utcnow()
    for i, form_row in forms.iterrows():
        form = form_row.get('objectId')
        name = form_row.get('name')
        description = form_row.get('description')
        is_custom_form = form_row.get('customForm')
        created_at = form_row.get('createdAt')
        updated_at = form_row.get('updatedAt')
        if form in [np.nan, None, '', " "]:
            #log the bad forms here
            continue
        uuid = md5_encode(form)
        #print(uuid)
        #print(name)
        #print(description)
        #print(is_custom_form)
        cur.execute(
                f"""
                INSERT INTO form_dim (uuid, name, description, is_custom_form, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (uuid, name, description, is_custom_form, created_at, updated_at)
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
    survey_orgs = df['surveyingOrganization'].unique()
    now = datetime.datetime.utcnow()
    for survey_org in survey_orgs:
        if survey_org in [np.nan, None, '', ' ']:
            #log bad survey orgs here
            continue
        #print('survey org')
        #print(survey_org)
        uuid = md5_encode(survey_org)
        cur.execute(
                f"""
                INSERT INTO surveying_organization_dim (uuid, name, created_at, updated_at)
                VALUES (%s, %s, %s, %s)
                """,
                (uuid, survey_org, now, now)
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
    users = unique_combos(df, ['objectId', 'username', 'firstname', 'lastname', 'phonenumber', 'role', 'createdAt', 'updatedAt', 'organization'])
    users = coalesce_pkey(users, 'objectId')
    users = add_surveyuser_column(users)
    for i, user_row in users.iterrows():
        survey_user = user_row.get('survey_user')
        survey_org = user_row.get('organization')
        user_name = user_row.get('user_name')
        first_name = user_row.get('firstname')
        last_name = user_row.get('lastname')
        phone_number = user_row.get('phonenumber')
        role = user_row.get('role')
        created_at = user_row.get('createdAt')
        updated_at = user_row.get('updatedAt')
        if (survey_user is None)|(survey_org is None)|(survey_user in ['', ' '])|(survey_org in ['', ' '])|(user_name is None):
            continue
        #print('users')
        #print(survey_user)
        #print(user_name)
        #print(first_name, last_name)
        full_name = first_name + ' ' + last_name
        uuid = md5_encode(survey_user)
        survey_org = md5_encode(survey_org)
        cur.execute(
                f"""
                INSERT INTO users_dim (uuid, survey_user, user_name, first_name, last_name, created_at, updated_at, phone_number, role, surveying_organization_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (uuid, survey_user, user_name, first_name, last_name, created_at, updated_at, phone_number, role, survey_org)
            )

    # Commit the changes to the database
    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"users": users.to_json()}),
        "isBase64Encoded": False,
    }

def get_household_dim(df):
    con = connection()
    cur = con.cursor()
    households = unique_combos(df, ['householdId', 'latitude', 'longitude', 'communityname'])
    households = coalesce_pkey(households, 'householdId')
    #households = df[['householdId', 'latitude', 'longtitude', 'communityname']].unique()
    now = datetime.datetime.utcnow()
    for i, household_row in households.iterrows():
        household_id = household_row.get('householdId')
        community_name = household_row.get('communityname')
        lat = household_row.get('latitude')
        lon = household_row.get('longitude')
        if (household_id in ['', ' ', None, np.nan])|(community_name in ['', ' ', None, np.nan]):
            continue

        uuid = md5_encode(household_id)
        community = md5_encode(community_name)
        #print(uuid, lat, lon, now, community, community_name)
        cur.execute(
                f"""
                INSERT INTO household_dim (uuid, latitude, longitude, created_at, updated_at, community_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (uuid, lat, lon, now, now, community)
            )

    # Commit the changes to the database
    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"households": households.to_json()}),
        "isBase64Encoded": False,
    }

def get_patient_dim(df):
    #this data comes from surveyfact
    con = connection()
    cur = con.cursor()
    df['age'] = df['age'].replace({'nan': None, '': None, ' ': None, np.nan: None})
    patients = unique_combos(df, ['objectId', 'fname', 'lname', 'sex', 'age', 'nickname', 'telephoneNumber', 'householdId'])
    patients = coalesce_pkey(patients, 'objectId')
    #print('patients dtypes')
    #print(patients.dtypes)

    missing_rows = []
    
    #patients = df[].unique()
    now = datetime.datetime.utcnow()
    for i, patient_row in patients.iterrows():
        patient_id = patient_row.get('objectId')
        household_id = patient_row.get('householdId')
        first_name = patient_row.get('fname')
        last_name = patient_row.get('lname')
        nick_name = patient_row.get('nickname')
        sex = patient_row.get('sex')
        age = patient_row.get('age')
        if age in ['11 meses']:
            age = '0'
        phone_number = patient_row.get('telephoneNumber')
        if (patient_id in ['', ' ', None, np.nan])|(household_id in ['', ' ', None, np.nan]):
            continue
        #if patient_id == '4ABNhV9swN':
            #print(patient_id, patient_id, household_id, first_name, last_name, age, phone_number)
        uuid = md5_encode(patient_id)
        household_uuid = md5_encode(household_id)
        try:
            cur.execute(
                    f"""
                    INSERT INTO patient_dim (uuid, first_name, last_name, nick_name, sex, age, created_at, updated_at, phone_number, household_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (uuid, first_name, last_name, nick_name, sex, age, now, now, phone_number, household_uuid)
                )
        except ForeignKeyViolation:
            print('foreign key violation')
            missing_rows.append((patient_id, household_id, household_uuid))
            print(patient_id, household_id, household_uuid)
            cur.execute("ROLLBACK")
            continue

    missing_rows_df = pd.DataFrame.from_records(missing_rows)
    missing_rows_df.columns = ['patient_id', 'household_id', 'household_uuid']
    missing_rows_df.to_csv('./patient_dim.csv', index=False)
            

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
    #this comes from formspecificationsv2
    con = connection()
    cur = con.cursor()

    #grouping by fields returns nan for some reason??
    unique_forms = unique_combos(df, ['objectId', 'createdAt', 'updatedAt'])
    unique_forms = coalesce_pkey(unique_forms, 'objectId')
    forms = unique_forms.merge(df[['objectId', 'fields', 'createdAt', 'updatedAt']], on=['objectId', 'createdAt', 'updatedAt'])
    inserted_uuids = []
    for i, form_row in forms.iterrows():
        form = form_row.get('objectId')
        form_created_at = form_row.get('createdAt')
        form_updated_at = form_row.get('updatedAt')
        if (form is None)|(form in ['', ' ']):
            continue
        form_id = md5_encode(form)

        #print('form row')
        #print(form_row.index)
        #print(form_row)

        question_list = form_row.get('fields')
        #print('question list')
        #print(question_list)
        if isinstance(question_list, float):
         #   print('float questions list')
         #   print(question_list)
            continue
        for question in question_list:
            uuid = question.get('id')
            field_type = question.get('fieldType')
            formik_key = question.get("formikKey")
            question_label = question.get('label')
            if (uuid in ['', ' ', None, np.nan])|(question_label in ['', ' ', None, np.nan]):
                continue
            #note sure the best way to handle this
            if field_type in ['select', 'selectMulti']:
                options = question.get('options')
                options_list = [option['label'] for option in options]
            else:
                options_list = None

            if uuid in inserted_uuids:
                continue
            else:
                cur.execute(
                    f"""
                    INSERT INTO question_dim (uuid, question, field_type, formik_key, options, created_at, updated_at, form_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, 
                    (uuid, question_label, field_type, formik_key, options_list, form_created_at, form_updated_at, form_id)
                )
                inserted_uuids.append(uuid)

    # Commit the changes to the database
    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()

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
                (uuid, name, description, False, now, now)
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

    # id_cols = [
    #     'objectId',
    #     'ACL',
    #     'client',
    #     'createdAt',
    #     'updatedAt',
    #     'parseParentClassObjectIdOffline',
    #     'phoneOS',
    #     'surveyingUser',
    #     'appVersion',
    #     'surveyingOrganization',
    #     'parseUser'
    # ]

    # nosql_table_questions = [col for col in nosql_table.columns if col not in id_cols]
    for question_tuple in config:
        uuid, label, formik_key, field_type, options = question_tuple#
        uuid = md5_encode(formik_key)
        #formik_key = to_camel_case(question_name)
        cur.execute(
                f"""
                INSERT INTO question_dim (uuid, question, field_type, formik_key, options, created_at, updated_at, form_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (uuid, label, field_type, formik_key, options, now, now, form_id)
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
                'yearsLivedinthecommunity': 'yearsLivedinthecommunitySupplementary',
                'yearsLivedinThisHouse': 'yearsLivedinThisHouseSupplementary',
                'waterAccess': 'waterAccessSupplementary',
                'numberofIndividualsLivingintheHouse': 'numberofIndividualsLivingintheHouseSupplementary'
            }
    import time
    for i in range(1,11):
        print(i)
        time.sleep(5)
    nosql_df = restCall(table_name, None).rename(rename_dict, axis=1)
    #print('nosql')
    #print(nosql_df.columns)
    #print(nosql_df)
    id_cols = ['objectId', 'surveyingOrganization', 'surveyingUser', 'communityname', 'householdId', 'createdAt', 'updatedAt']
    merged = survey_df.merge(nosql_df, on='objectId')
    # both_df_cols = ['surveyingOrganization', 'surveyingUser', 'createdAt', 'updatedAt']
    # for col in both_df_cols:
    #     merged[col] = merged[col].combine_first(merged[f'{col}_y'])
    #     merged.drop([f'{col}_x', f'{col}_y'], inplace=True, axis=1)
    #print('merged')
    #print(merged.columns)
    #print(merged)
    
    config = parse_json_config(CONFIGS[table_name])
    #print('config')
    #print(config)
    questions = []
    for question_tuple in config:
    #    print(question_tuple)
        _, _, formik_key, _, _ = question_tuple
        questions.append(formik_key)

#    print('questions')
#    print(questions)
#    print(merged.columns)
    questions = [question for question in questions if question in list(merged.columns)]
    comb_df = merged[id_cols+questions].melt(id_vars=id_cols, var_name='question', value_name='answer')
 #   print('comb df')
 #   print(comb_df)


    ignore_questions = ['searchIndex'] + [col for col in questions if 'location' in col]

    fk_missing_rows = []
    notnull_missing_rows = []

    for i, row in comb_df.iterrows():
        # for question_tuple in config:
        #     _, _, formik_key, _, _ = question_tuple
        created_at = row['createdAt']
        updated_at = row['updatedAt']
        question_name = row['question']
        if question_name in ignore_questions:
            continue
        question_answer = row['answer']
        object_id = row['objectId']
        survey_org = row['surveyingOrganization']
        user = row['surveyingUser']
        community_name = row['communityname']
        nosql_household_id = row['householdId']
        #theres probably an elegant way to do this but whatever im tired
        #for id in [object_id, survey_org, user, community_name]
        if (object_id in ['', ' ',None, np.nan])|(survey_org in ['', ' ',None, np.nan])|(user in ['', ' ',None, np.nan])|(community_name in ['', ' ',None, np.nan])|(nosql_household_id in ['', ' ',None, np.nan])|(question_name in ['', ' ',None, np.nan]):
            continue
        patient_id = md5_encode(object_id)
        surveying_organization_id = md5_encode(survey_org)
        user_id = str(md5_encode(user))
        community_id = md5_encode(community_name)
        household_id = md5_encode(nosql_household_id)
        question_id = md5_encode(question_name)
        form_id = md5_encode(table_name)

        #composite_key = surveying_organization_id + user_id + community_id + question_id + form_id + patient_id + household_id
        #uuid = md5_encode(composite_key)
        id = str(uuid.uuid4())
        #user_id = 
        
        #print('survey fact insert')
        #print(survey_org, user, community_name, nosql_household_id, question_name, object_id)
        insert_tuple = (id, surveying_organization_id, user_id, community_id, question_id, question_answer, created_at, updated_at, patient_id, household_id, form_id)
        #for thing in [id, surveying_organization_id, user_id, community_id, question_id, question_answer, created_at, updated_at, patient_id, household_id, form_id]:
        #    print(type(thing))
        try:
            cur.execute(
            f"""
            INSERT INTO survey_fact (uuid, surveying_organization_id, surveying_user_id, community_id, question_id, question_answer, created_at, updated_at, patient_id, household_id, form_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (id, surveying_organization_id, user_id, community_id, question_id, question_answer, created_at, updated_at, patient_id, household_id, form_id)
        )
        
        except ForeignKeyViolation:
            print('foreign key violation')
            fk_missing_rows.append(insert_tuple)
            print(insert_tuple)
            cur.execute("ROLLBACK")
            continue

        except NotNullViolation:
            print('not null violation')
            notnull_missing_rows.append(insert_tuple)
            print(insert_tuple)
            cur.execute("ROLLBACK")
            continue

        #except ProgrammingError:
        #    print('programming error')
        #    notnull_missing_rows.append(insert_tuple)
        #    print(insert_tuple)
        #    cur.execute("ROLLBACK")
            # Commit the changes to the database
    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()

    cols = [
        'uuid',
        'surveying_organization_id',
        'user_id',
        'community_id',
        'question_id',
        'question_answer',
        'created_at',
        'updated_at',
        'patient_id',
        'household_id',
        'form_id'
        ]

    notnull_missing_rows_df = pd.DataFrame.from_records(notnull_missing_rows, columns=cols)
    fk_missing_rows_df = pd.DataFrame.from_records(fk_missing_rows, columns=cols)

    print('nn')
    print(notnull_missing_rows_df)
    print('fk')
    print(fk_missing_rows)
    
    #notnull_missing_rows_df.columns = cols
    #fk_missing_rows_df.columns = cols
    
    notnull_missing_rows_df.to_csv('./add_nosql_to_fact_notnull.csv', index=False)
    fk_missing_rows_df.to_csv('./add_nosql_to_fact_fk.csv', index=False)

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"questions": comb_df.to_json()}),
        "isBase64Encoded": False,
    }

            
def fill_tables():
    survey_df = restCall('SurveyData', None) #get this data from existing database
   #print('survey df')
   # print(survey_df[survey_df['objectId']=='4ABNhV9swN'])
    get_community_dim(survey_df)
    get_surveying_organization_dim(survey_df)
    form_specs = restCall('FormSpecificationsV2', None)
    #print('form specss')
    #print(form_specs)
    #print(form_specs.dtypes)
    get_form_dim(form_specs)
    users_df = restCall('users', None)
    #users_df.to_csv('users_test.csv')
    #print('users df')
    #print(users_df)
    #print(users_df.columns)
    get_users_dim(users_df)
    get_household_dim(survey_df)
    get_patient_dim(survey_df)
    get_question_dim(form_specs)

    for table_name, table_desc in NOSQL_TABLES.items():
        now = datetime.datetime.now()
        #print('table name, desc')
        #print(table_name, table_desc)
        #rest call here to get appropriate table
        #nosql_df = restCall(table_name, None)
        #print('nosql df')
        #print(nosql_df)
        #config = parse_json_config(CONFIGS[table_name])
        #print('config')
        #print(config)
        add_nosql_to_forms(table_name, table_desc, now)
        ingest_nosql_table_questions(table_name)

        add_nosql_to_fact(table_name, survey_df)

        #get_survey_fact(nosql_df, survey_df)


def create_tables():
    initialize_tables()
    fill_tables()

drop_tables()
initialize_tables()
fill_tables()