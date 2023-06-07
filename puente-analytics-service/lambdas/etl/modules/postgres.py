import os
import psycopg2
import datetime
import json

#with open("survey_data_config.json", 'r') as j:
#     contents = json.loads(j.read())
#     print(contents)
#print(os.getcwd())
#survey_config = json.loads("./survey_data_config.json")
#env_config = json.loads('env_data_config.json')

import hashlib

#survey_config

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

PG_HOST = os.environ.get('PG_HOST')
PG_PORT = os.environ.get('PG_PORT')
PG_DATABASE = os.environ.get('PG_DATABASE')
PG_USERNAME = os.environ.get('PG_USERNAME')
PG_PASSWORD = os.environ.get('PG_PASSWORD')

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

def connection():
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USERNAME,
        password=PG_PASSWORD
    )
    return conn

def initialize_tables():
    conn = connection()
    cur = conn.cursor()
    users_q = f"""
    CREATE TABLE IF NOT EXISTS users_dim (
        uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        email VARCHAR(255),
        phone_number VARCHAR(255),
        suveying_organization_id UUID NOT NULL REFERENCES surveying_organization_dim (uuid)
    );
    """

    surveying_org_q = f"""
    CREATE TABLE surveying_organization_dim (
        uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        name VARCHAR(255) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """

    community_q = f"""
    CREATE TABLE community_dim (
        uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        name VARCHAR(255) NOT NULL,
        city VARCHAR(255),
        region VARCHAR(255),
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """

    household_q = f"""
    CREATE TABLE household_dim (
        uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        latitude NUMERIC(9,6) NOT NULL,
        longtitude NUMERIC(9,6) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        community_id UUID NOT NULL REFERENCES community_dim (uuid)
    );
    """

    patient_q = f"""
    CREATE TABLE patient_dim (
        uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        nick_name VARCHAR(255), 
        sex VARCHAR(255),
        phone_number VARCHAR(255),
        email VARCHAR(255),
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        household_id UUID NOT NULL REFERENCES household_dim (uuid)
    )
    """

    form_q = f"""
    CREATE TABLE form_dim (
        uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        name VARCHAR(255) NOT NULL,
        description VARCHAR(255),
        is_custom_form BOOLEAN NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """

    question_q = f"""
    CREATE TABLE question_dim (
        uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        question VARCHAR(255) NOT NULL,
        field_type VARACHAR(255) NOT NULL,
        formik_key VARCHAR(255),
        options TEXT[],
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        form_id UUID NOT NULL REFERENCES form_dim (uuid)
    );
    """

    survey_fact_q = f"""
    CREATE TABLE survey_fact (
        uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
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
    for q in create_qs:
        cur.execute(q)
        # Commit the changes to the database
        conn.commit()

    # Close the database connection and cursor
    cur.close()
    conn.close()

def get_community_dim(df):
    con = connection()
    cur = con.cursor()
    communities = df[['communityname', 'city', 'region']].unique()
    now = datetime.datetime.utcnow()
    for community_row in communities:
        community = community_row['communityname']
        city = community_row['city']
        region = community_row['region']
        uuid = md5_encode(community)

        cur.execute(
                f"""
                INSERT INTO community_dim (uuid, name, city, region, created_at, updated_at)
                VALUES ({uuid}, {community}, {city}, {region}, {now}, {now})
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
        "body": json.dumps({"communities": communities}),
        "isBase64Encoded": False,
    }

def get_form_dim(df):
    #this comes from formspecificationsv2
    con = connection()
    cur = con.cursor()
    forms = df[['objectId', 'name', 'description', 'customForm', 'createdAt', 'updatedAt']].unique()
    now = datetime.datetime.utcnow()
    for form_row in forms:
        form = form_row['objectId']
        name = form_row['name']
        description = form_row['description']
        is_custom_form = form_row['customForm']
        created_at = form_row['createdAt']
        updated_at = form_row['updatedAt']
        uuid = md5_encode(form)
        cur.execute(
                f"""
                INSERT INTO form_dim (uuid, name, description, is_custom_form, created_at, updated_at)
                VALUES ({uuid}, {name}, {description}, {is_custom_form}, {created_at}, {updated_at})
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
        "body": json.dumps({"forms": forms}),
        "isBase64Encoded": False,
    }

def get_surveying_organization_dim(df):
    con = connection()
    cur = con.cursor()
    survey_orgs = df['surveyingOrganization'].unique()
    now = datetime.datetime.utcnow()
    for survey_org in survey_orgs:
        uuid = md5_encode(survey_org)
        cur.execute(
                f"""
                INSERT INTO surveying_organization_dim (uuid, name, created_at, updated_at)
                VALUES ({uuid}, {survey_org}, {now}, {now})
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
        "body": json.dumps({"surveying_organizations": survey_orgs}),
        "isBase64Encoded": False,
    }

def get_users_dim(df):
    con = connection()
    cur = con.cursor()
    users = df[['surveyingUser', 'fname', 'lname', 'phone_number', 'email', 'surveyingOrganization']].unique()
    now = datetime.datetime.utcnow()
    for user_row in users:
        user = user_row['surveyingUser']
        survey_org = user_row['SurveyingOrganization']
        first_name = user_row['fname']
        last_name = user_row['lname']
        phone_number = user_row['phone_number']
        email = user_row['email']
        uuid = md5_encode(user)
        survey_org = md5_encode(survey_org)
        cur.execute(
                f"""
                INSERT INTO users_dim (uuid, first_name, last_name, created_at, updated_at, phone_number, email, surveying_organization_id)
                VALUES ({uuid}, {first_name}, {last_name}, {now}, {now}, {phone_number}, {email}, {survey_org})
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
        "body": json.dumps({"users": users}),
        "isBase64Encoded": False,
    }

def get_household_dim(df):
    con = connection()
    cur = con.cursor()
    households = df[['householdId', 'latitude', 'longtitude', 'communityname']].unique()
    now = datetime.datetime.utcnow()
    for household_row in households:
        household_id = household_row['householdId']
        community_name = household_row['communityname']
        lat = household_row['latitude']
        lon = household_row['longitude']
        uuid = md5_encode(household_id)
        community = md5_encode(community_name)
        cur.execute(
                f"""
                INSERT INTO household_dim (uuid, latitude, longitude, created_at, updated_at, community_id)
                VALUES ({uuid}, {lat}, {lon}, {now}, {now}, {community})
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
        "body": json.dumps({"households": households}),
        "isBase64Encoded": False,
    }

def get_patient_dim(df):
    #this data comes from surveyfact
    con = connection()
    cur = con.cursor()
    patients = df[['objectId', 'surveyingUser', 'fname', 'lname', 'sex', 'nickname' 'phone_number', 'email', 'householdId']].unique()
    now = datetime.datetime.utcnow()
    for patient_row in patients:
        patient_id = patient_row['objectId']
        household_id = patient_row['householdId']
        first_name = patient_row['fname']
        last_name = patient_row['lname']
        nick_name = patient_row['nickname']
        sex = patient_row['sex']
        phone_number = patient_row['phone_number']
        email = patient_row['email']
        uuid = md5_encode(patient_id)
        household_uuid = md5_encode(household_id)
        cur.execute(
                f"""
                INSERT INTO patient_dim (uuid, first_name, last_name, nick_name, sex, created_at, updated_at, phone_number, email, household_id)
                VALUES ({uuid}, {first_name}, {last_name}, {nick_name}, {sex}, {now}, {now}, {phone_number}, {email}, {household_uuid})
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
        "body": json.dumps({"patients": patients}),
        "isBase64Encoded": False,
    }

def get_question_dim(df):
    #this comes from formspecificationsv2
    con = connection()
    cur = con.cursor()
    forms = df[['objectId', 'fields', 'createdAt', 'updatedAt']].unique()
    for form_row in forms:
        form = form_row['objectId']
        form_created_at = form_row['createdAt']
        form_updated_at = form_row['updatedAt']
        form_id = md5_encode(form)

        question_list = form_row['fields']
        for question in question_list:
            uuid = question['id']
            field_type = question['fieldType']
            formik_key = question[ "formikKey"]
            question = question['label']
            #note sure the best way to handle this
            if field_type in ['select', 'selectMulti']:
                options = question['options']
            else:
                options = None
            cur.execute(
                f"""
                INSERT INTO question_dim (uuid, question, field_type, formik_key, options, created_at, updated_at, form_id)
                VALUES ({uuid}, {question}, {field_type}, {formik_key}, {options}, {form_created_at}, {form_updated_at}, {form_id})
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
    #survey_df = restCall() #get this data from existing database
    get_community_dim(df)
    get_surveying_organization_dim(df)
    get_form_dim(df)
    get_users_dim(df)
    get_household_dim(df)
    get_patient_dim(df)
    get_question_dim(df)

    for table_name, table_desc in NOSQL_TABLES.items():
        #rest call here to get appropriate table
        add_nosql_to_forms(table_name, table_desc)
        ingest_nosql_table_questions(nosql_df)
    get_survey_fact(df)


def create_tables():
    initialize_tables()
    fill_tables()