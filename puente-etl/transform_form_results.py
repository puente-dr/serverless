import pandas as pd
from dotenv import load_dotenv; load_dotenv()

from load_from_s3 import load_pickle_list_from_s3
from utils.clients import Clients
from utils.helpers import \
    get_fields_from_list_of_dicts, \
    to_snake_case

pd.options.display.max_rows = 1000
pd.options.display.max_columns = 100
pd.options.display.max_colwidth = 100
pd.options.display.width = 4000


def get_form_results_df():

    full_table_data: list = load_pickle_list_from_s3(Clients.S3, 'form_results')

    # Denormalize Form Results
    form_results_df = denormalize_form_results(full_table_data)
    print(form_results_df)
    # questions_df = denormalize_questions(full_table_data)
    # print(questions_df)
    #
    # answers_df = denormalize_answers(questions_df=questions_df)
    #
    # df = form_results_df \
    #     .merge(questions_df, on='custom_form_id') \
    #     .drop(columns='question_options') \
    #     .merge(answers_df, on='question_id')
    # print(df)


def denormalize_form_results(forms_data: list):

    # Remove "location" which we do not need.
    cols = get_fields_from_list_of_dicts(forms_data)
    if 'location' in cols:
        cols.remove('location')

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

    print('Denormalized Form Results: ')
    print(tabulate(df, headers=df.columns))

    return df


if __name__ == '__main__':
    get_form_results_df()
