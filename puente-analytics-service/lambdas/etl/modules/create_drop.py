from utils import connection

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

def initialize_tables():
    conn = connection()
    cur = conn.cursor()
    users_q = f"""
    CREATE TABLE IF NOT EXISTS users_dim (
        uuid UUID PRIMARY KEY,
        survey_user VARCHAR(255) NOT NULL,
        user_name VARCHAR(255),
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
        household_id UUID REFERENCES household_dim (uuid)
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
        question_answer VARCHAR(10000) NOT NULL,
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
