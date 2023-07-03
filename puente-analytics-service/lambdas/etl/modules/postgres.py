import os
import psycopg2
import datetime
import json
import numpy as np

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
        print('user url')
        print(url)
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
            uuid = md5_encode(formikkey)
            field_type = question['fieldType']
            if 'options' in question.keys():
                options = [option['value'] for option in question['options']]
            else:
                options = None

            question_values = (uuid, formikkey, formikkey, field_type, options)
            question_values_list.append(question_values)

    return question_values_list

# PG_HOST = os.environ.get('PG_HOST')
# PG_PORT = os.environ.get('PG_PORT')
# PG_DATABASE = os.environ.get('PG_DATABASE')
# PG_USERNAME = os.environ.get('PG_USERNAME')
# PG_PASSWORD = os.environ.get('PG_PASSWORD')

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
        print('query:')
        print(name)
        cur.execute(q)
        # Commit the changes to the database
        conn.commit()

    # Close the database connection and cursor
    cur.close()
    conn.close()

def get_community_dim(df):
    con = connection()
    cur = con.cursor()
    communities = unique_combos(df, ['communityname', 'city', 'region'])
    communities = coalesce_pkey(communities, 'communityname')
    now = datetime.datetime.utcnow()
    for i, community_row in communities.iterrows():
        print('community')
        print(community_row)
        community = community_row.get('communityname')
        city = community_row.get('city')
        region = community_row.get('region')
    
        if (community is None)|(community in ["", " "]):
            print('continuing')
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
        if (form is None)|(form in ['', " "]):
            continue
        uuid = md5_encode(form)
        print(uuid)
        print(name)
        print(description)
        print(is_custom_form)
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
        if (survey_org is None)|(survey_org in ['', ' ']):
            continue
        print('survey org')
        print(survey_org)
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
    for i, user_row in users.iterrows():
        user = user_row.get('objectId')
        survey_org = user_row.get('organization')
        user_name = user_row.get('user_name')
        first_name = user_row.get('firstname')
        last_name = user_row.get('lastname')
        phone_number = user_row.get('phonenumber')
        role = user_row.get('role')
        created_at = user_row.get('createdAt')
        updated_at = user_row.get('updatedAt')
        if (user is None)|(survey_org is None)|(user in ['', ' '])|(survey_org in ['', ' '])|(user_name is None):
            continue
        print('users')
        print(user)
        print(user_name)
        print(first_name, last_name)
        uuid = md5_encode(user)
        survey_org = md5_encode(survey_org)
        cur.execute(
                f"""
                INSERT INTO users_dim (uuid, user_name, first_name, last_name, created_at, updated_at, phone_number, role, surveying_organization_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (uuid, user_name, first_name, last_name, created_at, updated_at, phone_number, role, survey_org)
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
        if (household_id is None)|(community_name is None)|(household_id in ['', ' '])|(community_name in ['', ' ']):
            continue
        uuid = md5_encode(household_id)
        community = md5_encode(community_name)
        print(uuid, lat, lon, now, community, community_name)
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
    patients = unique_combos(df, ['objectId', 'fname', 'lname', 'sex', 'age', 'nickname', 'telephoneNumber', 'householdId'])
    patients = coalesce_pkey(patients, 'objectId')
    print('patients dtypes')
    print(patients.dtypes)
    df['age'] = df['age'].replace({'nan': None, '': None, ' ': None, np.nan: None})
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
        phone_number = patient_row.get('telephoneNumber')
        if (patient_id is None)|(household_id is None)|(patient_id in ['', ' '])|(household_id in ['', ' ']):
            continue
        print(patient_id, household_id, first_name, last_name, age, phone_number)
        uuid = md5_encode(patient_id)
        household_uuid = md5_encode(household_id)
        cur.execute(
                f"""
                INSERT INTO patient_dim (uuid, first_name, last_name, nick_name, sex, age, created_at, updated_at, phone_number, household_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (uuid, first_name, last_name, nick_name, sex, age, now, now, phone_number, household_uuid)
            )

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
    patients = unique_combos(df, ['objectId', 'fname', 'lname', 'sex', 'age', 'nickname', 'telephoneNumber', 'householdId'])
    patients = coalesce_pkey(patients, 'objectId')
    forms = unique_combos(df, ['objectId', 'fields', 'createdAt', 'updatedAt'])
    for i, form_row in forms.iterrows():
        form = form_row.get('objectId')
        form_created_at = form_row.get('createdAt')
        form_updated_at = form_row.get('updatedAt')
        if (form is None)|(form in ['', ' ']):
            continue
        form_id = md5_encode(form)

        question_list = form_row.get('fields')
        for question in question_list:
            uuid = question.get('id')
            field_type = question.get('fieldType')
            formik_key = question.get("formikKey")
            question = question.get('label')
            #note sure the best way to handle this
            if field_type in ['select', 'selectMulti']:
                options = question.get('options')
            else:
                options = None
            cur.execute(
                f"""
                INSERT INTO question_dim (uuid, question, field_type, formik_key, options, created_at, updated_at, form_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, 
                (uuid, question, field_type, formik_key, options, form_created_at, form_updated_at, form_id)
            )

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
                VALUES ({uuid}, {name}, {description}, {False}, {now}, {now})
                """
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

def ingest_nosql_table_questions(nosql_table, table_name):

    con = connection()
    cur = con.cursor()

    now = datetime.datetime.now()

    form_id = md5_encode(table_name)

    id_cols = [
        'objectId',
        'ACL',
        'client',
        'createdAt',
        'updatedAt',
        'parseParentClassObjectIdOffline',
        'phoneOS',
        'surveyingUser',
        'appVersion',
        'surveyingOrganization',
        'parseUser'
    ]

    nosql_table_questions = [col for col in nosql_table.columns if col not in id_cols]
    for question_name in nosql_table_questions:
        uuid = md5_encode(question_name)
        formik_key = to_camel_case(question_name)
        cur.execute(
                f"""
                INSERT INTO question_dim (uuid, question, field_type, formik_key, options, created_at, updated_at, form_id)
                VALUES ({uuid}, {question_name}, {field_type}, {formik_key}, {options}, {now}, {now}, {form_id})
                """
            )

            # Commit the changes to the database
    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"questions": nosql_table_questions}),
        "isBase64Encoded": False,
    }

def get_survey_fact(env_df):
    env_df_questions = env_df.melt(id_vars=id_cols, var_name='question', value_name='answer')

    env_df['community_id'] = env_df['communityname'].apply(md5_encode)
    env_df['patient_id'] = env_df['client'].apply(md5_encode)
    env_df['household_id'] = env_df['householdId'].apply(md5_encode)
    env_df['surveyorg_id'] = env_df['surveyingOrganization'].apply(md5_encode)
    env_df['user_id'] = env_df['surveryingUser'].apply(md5_encode)
    pass


def fill_tables():
    survey_df = restCall('SurveyData', None) #get this data from existing database
    #print('survey df')
    #print(survey_df)
    get_community_dim(survey_df)
    get_surveying_organization_dim(survey_df)
    form_specs = restCall('FormSpecificationsV2', None)
    print('form specss')
    print(form_specs)
    get_form_dim(form_specs)
    users_df = restCall('users', None)
    #users_df.to_csv('users_test.csv')
    print('users df')
    print(users_df)
    print(users_df.columns)
    get_users_dim(users_df)
    get_household_dim(survey_df)
    get_patient_dim(survey_df)
    get_question_dim(form_specs)

    # for table_name, table_desc in NOSQL_TABLES.items():
    #     #rest call here to get appropriate table
    #     add_nosql_to_forms(table_name, table_desc)
    #     ingest_nosql_table_questions(nosql_df)
    # get_survey_fact(df)


def create_tables():
    initialize_tables()
    fill_tables()

drop_tables()
initialize_tables()
fill_tables()