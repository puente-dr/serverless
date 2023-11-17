import os
import datetime

import os
import sys; sys.path.append(os.path.join(os.path.dirname(__file__)))

from create_drop import drop_tables, initialize_tables, get_existing_tables
from env_utils import NOSQL_TABLES
from utils import restCall, query_bronze_layer

from dimensions import (
    get_community_dim,
    get_surveying_organization_dim,
    get_form_dim,
    get_users_dim,
    get_household_dim,
    get_patient_dim,
    get_question_dim,
    add_nosql_to_forms,
    ingest_nosql_table_questions
)

from facts import (
    add_nosql_to_fact,
    get_custom_forms
)

            
def fill_tables():
#     #survey_df = restCall('SurveyData', None) #get this data from existing database
    survey_df = query_bronze_layer("SurveyData")
#     print('survey df')
#    # print(survey_df[survey_df['objectId']=='4ABNhV9swN'])
#     get_community_dim(survey_df)
#     print('community dim')
#     get_surveying_organization_dim(survey_df)
#     print('survey org dim')
#     #form_specs = restCall('FormSpecificationsV2', None)
#     form_specs = query_bronze_layer("FormSpecificationsV2")
#     print('form specs')
#     #print(form_specs)
#     #print(form_specs.dtypes)
#     get_form_dim(form_specs)
#     print('form dim')
#     #users_df = restCall('users', None)
#     users_df = query_bronze_layer("users")
#     print('users')
#     #users_df.to_csv('users_test.csv')
#     #print('users df')
#     #print(users_df)
#     #print(users_df.columns)
#     get_users_dim(users_df)
#     print('users dim')
#     get_household_dim(survey_df)
#     print('household dim')
#     get_patient_dim(survey_df)
#     print('patient dim')
#     get_question_dim(form_specs)
#     print('question dim')

#     for table_name, table_desc in NOSQL_TABLES.items():
#         now = datetime.datetime.now()
#         print('table name, desc')
#         print(table_name, table_desc)
#         #rest call here to get appropriate table
#         #nosql_df = restCall(table_name, None)
#         #print('nosql df')
#         #print(nosql_df)
#         #config = parse_json_config(CONFIGS[table_name])
#         #print('config')
#         #print(config)
#         add_nosql_to_forms(table_name, table_desc, now)
#         print('add nosql to forms')
#         ingest_nosql_table_questions(table_name)
#         print('insert qs')

#         add_nosql_to_fact(table_name, survey_df)
#         print('add to fact')

#         #get_survey_fact(nosql_df, survey_df)

    #form_results = restCall('FormResults', None)
    form_results = query_bronze_layer("FormResults")
    print('form results')
    #print(form_results.columns)
    #print(form_results['fields'].values[0])
    #print(form_results[[col for col in form_results.columns if 'client' in col]].head())
    form_results = form_results.merge(survey_df[['objectId', 'communityname', 'householdId']].rename({'householdId': 'household', 'objectId': 'client.objectId'}, axis=1), on='client.objectId')
    #print(form_results.columns)
    #print(form_results[form_results['surveyingUser'].isnull()])
    get_custom_forms(form_results)
    print('custom forms')

def create_tables():
    #initialize_tables()
    fill_tables()

#drop_tables()
create_tables()

#get_existing_tables()