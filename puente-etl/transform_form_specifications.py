import pandas as pd
from dotenv import load_dotenv; load_dotenv()

from load_from_s3 import load_pickle_list_from_s3
from utils.clients import Clients
from utils.constants import ColumnOrder
from utils.helpers import \
    get_column_order_by_least_null, \
    get_fields_from_list_of_dicts, \
    get_unique_fields_from_list, \
    shortuuid_random, \
    to_snake_case

pd.options.display.max_rows = 1000
pd.options.display.max_columns = 100
pd.options.display.max_colwidth = 100
pd.options.display.width = 2000


def get_custom_form_schema_df():

    full_table_data: list = load_pickle_list_from_s3(Clients.S3, 'form_specifications_v2')

    # Denormalize Custom Form JSON
    form_df = denormalize_custom_forms(full_table_data)
    questions_df = denormalize_questions(full_table_data)
    answers_df = denormalize_answers(questions_df=questions_df)

    form_schema_df = form_df \
        .merge(questions_df, on='custom_form_id') \
        .drop(columns='question_options') \
        .merge(answers_df, on='question_id')

    form_schema_df = form_schema_df[ColumnOrder.FORM_SPECIFICATIONS]

    return form_schema_df


def denormalize_custom_forms(forms_data: list):

    # Remove nested "fields" which are denormalized elsewhere and "location" which we do not need.
    all_fields = get_fields_from_list_of_dicts(forms_data)
    cols = [
        c
        for c in all_fields
        if c not in ('fields', 'location')
    ]

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
        .add_prefix('custom_form_')

    return df


def denormalize_questions(forms_data: list):

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
        custom_form_id = form_data.get('_id')
        header = get_section_header(fields)

        for item in fields:
            row = [custom_form_id, header, shortuuid_random()]
            for col in cols:
                row.append(item.get(col))
            rows.append(row)

    # Prepend column for IDs and section Header
    # Format column names and add columns for Question ID and Answer ID
    cols_renamed = [f'question_{i}' for i in cols]
    cols_renamed.insert(0, 'custom_form_id')
    cols_renamed.insert(1, 'custom_form_header')
    cols_renamed.insert(2, 'question_id')
    cols_formatted = to_snake_case(cols_renamed)

    df = pd.DataFrame(rows, columns=cols_formatted)
    cols_ordered = get_column_order_by_least_null(df)
    df = df[cols_ordered]

    return df


def denormalize_answers(questions_df: pd.DataFrame):
    tmp_df = questions_df[['question_id', 'question_options']]

    # Exclude fieldTypes "header" and "numberInput"
    tmp_df = tmp_df[tmp_df['question_options'].notnull()]

    cols = []
    for options_list in tmp_df['question_options']:
        if options_list is not None:
            for option in options_list:
                # Get cols
                for fk in list(option.keys()):
                    if fk not in cols:
                        cols.append(fk)
    if 'id' in cols:
        cols.remove('id')

    rows = []
    for q_id, options_list in zip(tmp_df['question_id'], tmp_df['question_options']):
        if options_list is not None:
            for option in options_list:
                row = [q_id, shortuuid_random()]
                for col in cols:
                    row.append(option.get(col))
                rows.append(row)

    # Format column names and add columns for Question ID and Answer ID
    cols_renamed = [f'answer_{i}' for i in cols]
    cols_renamed.insert(0, 'question_id')
    cols_renamed.insert(1, 'answer_id')
    cols_formatted = to_snake_case(cols_renamed)

    df = pd.DataFrame(rows, columns=cols_formatted)

    return df


def get_section_header(fields_dict):
    for item in fields_dict:
        if item.get('fieldType') == 'header':
            return item.get('label')


if __name__ == '__main__':
    get_custom_form_schema_df()
