import pprint

import pandas as pd
from dotenv import load_dotenv; load_dotenv()

from load_from_s3 import load_dataframe_from_s3, load_pickle_list_from_s3
from utils.clients import Clients
from utils.helpers import \
    get_fields_from_list_of_dicts, \
    get_unique_fields_from_list, \
    to_snake_case

pd.options.display.max_rows = 1000
pd.options.display.max_columns = 100
pd.options.display.max_colwidth = 40
pd.options.display.width = 2000


def get_form_results_df():

    s3_client = Clients.S3

    full_table_data: list = load_pickle_list_from_s3(s3_client, 'form_results')

    pprint.pprint(full_table_data[0:3])

    custom_form_schema_df = load_dataframe_from_s3(s3_client, 'form_specifications_v2')

    # Denormalize Form Results
    form_results_df = denormalize_form_results(full_table_data)
    # print(form_results_df)

    q_df = denormalize_form_questions(full_table_data)
    print(q_df)

    #
    # TODO: question for standup
    #       looking at current exports, data is structured at the patient level
    #       across all tables. Goal here is aggregate the results of each survey?
    #       or retain individual patient responses?
    #
    #

    # # just for readability for now drop some of these
    # df = form_results_df[[
    #     'form_result_id',
    #     'form_result_description'
    #     'form_result_fields'
    #     'form_result_form_specifications_id'
    #     'form_result_title'
    # ]]

    # questions_df = denormalize_questions(full_table_data)
    # print(questions_df)
    #
    # answers_df = denormalize_answers(questions_df=questions_df)
    #
    # df = form_results_df \
    #     .merge(questions_df, on='custom_form_id') \
    #     .drop(columns='question_options') \
    #     .merge(answers_df, on='question_id')


def denormalize_form_results(forms_data: list):

    # Remove "location" which we do not need.
    cols = get_fields_from_list_of_dicts(forms_data)
    if 'location' in cols and '_p_loopClient' in cols:
        cols.remove('location')
        cols.remove('_p_loopClient')

    # Order Row Elements by Column
    data = []
    for form_data in forms_data:
        row = []
        for col in cols:
            row.append(form_data.get(col))
        data.append(row)

    # Clean Column Names: Remove leading underscore and apply snake_case
    column_names = [
        to_snake_case(col.lstrip('_'))
        for col in cols
    ]

    # Create DataFrame
    df = pd.DataFrame(data, columns=column_names) \
        .add_prefix('form_result_')

    return df


def denormalize_form_questions(forms_data: list):

    # Get unique nested fields across all data set
    fields_list = []
    for fields_data in forms_data:
        for cols_data in fields_data.get('fields'):
            fields_list.extend(list(cols_data.keys()))

    cols = get_unique_fields_from_list(fields_list)
    if 'id' in cols:
        cols.remove('id')

    rows = []
    for form_data in forms_data:
        fields = form_data.get('fields')
        form_result_question_id = form_data.get('_id')

        for item in fields:
            row = [form_result_question_id]
            for col in cols:
                row.append(item.get(col))
            rows.append(row)

    # Prepend column for ID
    cols_renamed = [f'question_{i}' for i in cols]
    cols_renamed.insert(0, 'form_result_question_id')
    cols_formatted = to_snake_case(cols_renamed)

    df = pd.DataFrame(rows, columns=cols_formatted)

    return df


if __name__ == '__main__':
    get_form_results_df()
