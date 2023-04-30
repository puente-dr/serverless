import psycopg2
import sys
import boto3
import os
import secretz



#gets the credentials from .aws/credentials
#session = boto3.Session(profile_name='RDSCreds')
client = boto3.client('rds', aws_access_key_id=secretz.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=secretz.AWS_SECRET_ACCESS_KEY, region_name=secretz.REGION)

try:
    conn = psycopg2.connect(host=secretz.ENDPOINT, port=secretz.PORT, database=secretz.DBNAME, user=secretz.USER, password=secretz.PW, sslrootcert="SSLCERTIFICATE")
    cur = conn.cursor()
except Exception as e:
    print("Database connection failed due to {}".format(e))
    
cur.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')

def query(sql):
    cur.execute(sql)
    query_results = cur.fetchall()
    print(query_results)
    return query_results

sql = """
CREATE TABLE my_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    age INTEGER
)
"""

def create_tables():
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
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """

    question_q = f"""
    CREATE TABLE question_dim (
        uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        question VARCHAR(255) NOT NULL,
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
        latitude NUMERIC(9,6) NOT NULL,
        longtitude NUMERIC(9,6) NOT NULL,
        city VARCHAR(255) NOT NULL,
        region VARCHAR(255) NOT NULL,
        patient_id UUID NOT NULL REFERENCES patient_dim (uuid),
        household_id UUID NOT NULL REFERENCES household_dim (uuid)
    );
    """

    create_qs = [surveying_org_q, users_q, community_q, household_q, patient_q, form_q, question_q, survey_fact_q]
    for q in create_qs:
        cur.execute(q)

#query("SELECT * FROM information_schema.tables")

create_tables()