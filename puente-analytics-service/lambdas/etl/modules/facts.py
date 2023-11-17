import pandas as pd
import numpy as np
import json
import uuid
from psycopg2.errors import ForeignKeyViolation
import time

from utils import get_subquestions, connection, md5_encode, restCall, parse_json_config, check_valid_field, query_bronze_layer
from env_utils import CONFIGS

def get_custom_forms(df):
    con = connection()
    cur = con.cursor()

    fk_missing_rows = []
    missing_qa_rows = []
    df['fields'] = df['fields'].apply(json.loads)
    exploded_df = df.explode('fields')

    exploded_df['fields'] = exploded_df['fields'].apply(lambda x: get_subquestions(x))
    exploded_df = exploded_df.explode('fields')

    exploded_df['title'] = exploded_df['fields'].apply(lambda x: x.get('title'))
    exploded_df['question_answer'] = exploded_df['fields'].apply(lambda x: x.get('answer'))

    #print(exploded_df.loc[exploded_df['question_answer'].isnull(), ['fields', 'title', 'question_answer']])

    missing_dict = {
        'hhids': [],
        'comms': [],
        'answers': [],
        'users': []
        }

    for i, row in exploded_df.iterrows():
        object_id = row.get('objectId')
        user = row.get('surveyingUser')
        survey_org = row.get('surveyingOrganization')
        form = row.get('formSpecificationsId')
        created_at = row.get('createdAt')
        updated_at = row.get('updatedAt')
        household = row.get('household')
        community_name = row.get('communityname')
        #questions = row.get('fields')
        title = row.get('title')
        question_answer = row.get('question_answer')

        if title == 'appVersion':
            continue

        #if len(str(question_answer)) > 250:
        #    print(question_answer)

        #print(object_id, survey_org, user, community_name)

        # if (object_id in ['', ' ',None, np.nan])|(survey_org in ['', ' ',None, np.nan])|(user in ['', ' ',None, np.nan])|(community_name in ['', ' ',None, np.nan]):
        #     continue

        # if household in [np.nan, None]:
        #     household = ' '

       #print('got past nulls')
        row_insert = (object_id, user, survey_org, form, household, community_name, title, question_answer)
        #print(row_insert)
        check_list = []
        for field in [household, community_name, question_answer, user]:
            if isinstance(field, str):
                check = 'test' in field.lower()
                check_list.append(check)
        if any(check_list):
            continue
                    
        if household in [None, np.nan]:
            household_id = None 
            # missing_dict['hhids'].append(row_insert)
            # continue
        else:
            household_id = md5_encode(household)

        if community_name in [None, np.nan]:
            missing_dict['comms'].append(row_insert)
            continue

        if question_answer in [None, np.nan]:
            missing_dict['answers'].append(row_insert)
            continue

        if user in [None, np.nan]:
            missing_dict['users'].append(row_insert)
            continue
        
        
        patient_id = md5_encode(object_id)
        user_id = md5_encode(user)
        surveying_organization_id = md5_encode(survey_org)
        form_id = md5_encode(form)
        community_id = md5_encode(community_name)

        #for title, question_answer in questions.items():
            
            #if isinstance(question, dict):
            #    for title, question in 
            #title = question['title']
        question_id = md5_encode(title)
        #question_answer = question['answer']

        id = str(uuid.uuid4())

        insert_tuple = (id, surveying_organization_id, user_id, community_id, question_id, question_answer, created_at, updated_at, patient_id, household_id, form_id)
        #for x in insert_tuple:
        #    if isinstance(x, dict):
        #        print('something was a dict')
        #        print(insert_tuple)
        #        print('the final insert tuple')
        #        return insert_tuple
        try:
            cur.execute(
            f"""
            INSERT INTO survey_fact (uuid, surveying_organization_id, surveying_user_id, community_id, question_id, question_answer, created_at, updated_at, patient_id, household_id, form_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (id, surveying_organization_id, user_id, community_id, question_id, question_answer, created_at, updated_at, patient_id, household_id, form_id)
        )
        
        except ForeignKeyViolation:
            insert_tuple = tuple(list(insert_tuple)[:5] + [title, object_id] + list(insert_tuple[5:]))
            #print('foreign key violation')
            fk_missing_rows.append(insert_tuple)
            #print(insert_tuple)
            cur.execute("ROLLBACK")
            continue

        # except NotNullViolation:
        #     insert_tuple = tuple(list(insert_tuple)[:5] + [title, object_id] + list(insert_tuple[5:]))
        #     missing_qa_rows.append(insert_tuple)
        #     cur.execute("ROLLBACK")
        #     continue

    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()

    cols = [
        'object_id',
        'user',
        'survey_org',
        'form',
        'household',
        'community',
        'question',
        'question_answer'
    ]

    for table, missing in missing_dict.items():
        missing_df = pd.DataFrame.from_records(missing, columns = cols)
        if missing_df.shape[0] > 0:
            missing_df.to_csv(f'customforms_missing_{table}.csv', index=False)

    cols = [
        'uuid',
        'surveying_organization_id',
        'user_id',
        'community_id',
        'question_id',
        'question_title',
        'objectId',
        'question_answer',
        'created_at',
        'updated_at',
        'patient_id',
        'household_id',
        'form_id'
        ]

    fk_missing_rows_df = pd.DataFrame.from_records(fk_missing_rows, columns = cols)
    if fk_missing_rows_df.shape[0] > 0:
        fk_missing_rows_df.to_csv('custom_fk.csv', index=False)
    missing_qa_rows_df = pd.DataFrame.from_records(missing_qa_rows, columns = cols)
    if missing_qa_rows_df.shape[0] > 0:
        missing_qa_rows_df.to_csv('custom_nullqa.csv', index=False)

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
                'yearsLivedinthecommunity': 'yearsLivedinthecommunitySupplementary',
                'yearsLivedinThisHouse': 'yearsLivedinThisHouseSupplementary',
                'waterAccess': 'waterAccessSupplementary',
                'numberofIndividualsLivingintheHouse': 'numberofIndividualsLivingintheHouseSupplementary'
            }
    
    # for i in range(1,11):
    #     print(i)
    #     time.sleep(15)

    print(table_name)
    #nosql_df = restCall(table_name, None).rename(rename_dict, axis=1)
    nosql_df = query_bronze_layer(table_name).rename(rename_dict, axis=1)
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

    missing_hhids = []
    missing_users = []
    missing_comms = []

    missing_dict = {
        'hhids': [],
        'users': [],
        'comms': [],
        'answers': []
        }

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
        # if (object_id in ['', ' ',None, np.nan])|(survey_org in ['', ' ',None, np.nan])|(user in ['', ' ',None, np.nan])|(community_name in ['', ' ',None, np.nan])|(nosql_household_id in ['', ' ',None, np.nan])|(question_name in ['', ' ',None, np.nan]):
        #     continue
        check_list = []
        for field in [nosql_household_id, user, community_name]:
            if isinstance(field, str):
                check = 'test' in field.lower()
                check_list.append(check)
        if any(check_list):
            continue

        row_insert = (object_id, survey_org, user, community_name, nosql_household_id, question_name, question_answer, table_name)
        if nosql_household_id in [None, np.nan]:
            household_id = None
        else:
            household_id = md5_encode(nosql_household_id)
            #missing_dict['hhids'].append(row_insert)
            #missing_hhids.append(row_insert)
            #continue
        if user in [None, np.nan]:
            missing_dict['users'].append(row_insert)
            #missing_users.append(row_insert)
            continue
        if community_name in [None, np.nan]:
            missing_dict['comms'].append(row_insert)
            #missing_comms.append(row_insert)
            continue
        if question_answer in [None, np.nan]:
            missing_dict['answers'].append(row_insert)
            continue
        patient_id = md5_encode(object_id)
        surveying_organization_id = md5_encode(survey_org)
        user_id = str(md5_encode(user))
        community_id = md5_encode(community_name)
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
            #print('foreign key violation')
            fk_missing_rows.append(insert_tuple)
            #print(insert_tuple)
            cur.execute("ROLLBACK")
            continue

        # except NotNullViolation:
        #     #print('not null violation')
        #     notnull_missing_rows.append(insert_tuple)
        #     #print(insert_tuple)
        #     cur.execute("ROLLBACK")
        #     continue

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

    #print('nn')
    #print(notnull_missing_rows_df)
    #print('fk')
    #print(fk_missing_rows)
    
    #notnull_missing_rows_df.columns = cols
    #fk_missing_rows_df.columns = cols

    cols = [
        'object_id',
        'survey_org',
        'user',
        'community_name',
        'household_id',
        'question_name',
        'question_answer',
        'table_name'
    ]

    for table, missing in missing_dict.items():
        missing_df = pd.DataFrame.from_records(missing, columns = cols)
        if missing_df.shape[0] > 0:
            missing_df.to_csv(f'add_nosql_to_fact_{table_name}_missing_{table}.csv', index=False)
    
    if notnull_missing_rows_df.shape[0] > 0:
        notnull_missing_rows_df.to_csv(f'./add_nosql_to_fact_notnull_{table_name}.csv', index=False)
    if fk_missing_rows_df.shape[0] > 0:
        fk_missing_rows_df.to_csv(f'./add_nosql_to_fact_fk_{table_name}.csv', index=False)

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"questions": comb_df.to_json()}),
        "isBase64Encoded": False,
    }