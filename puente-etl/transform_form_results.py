import pandas as pd
from dotenv import load_dotenv; load_dotenv()

from load_from_s3 import load_dataframe_from_s3, load_pickle_list_from_s3
from utils.clients import Clients
from utils.constants import ColumnOrder
from utils.helpers import \
    get_fields_from_list_of_dicts, \
    get_unique_fields_from_list, \
    shortuuid_random, \
    to_snake_case


def get_form_results_df(groupby_cols=[]):

    s3_client = Clients.S3

    survey_data = load_pickle_list_from_s3(s3_client, "survey_data")
    survey_df = pd.DataFrame(survey_data)

    custom_form_schema_df = load_dataframe_from_s3(s3_client, 'form_specifications_v2')
    custom_form_schema_df = custom_form_schema_df[ColumnOrder.FORM_SPECIFICATIONS]
    # custom_form_schema_df.to_csv('./custom_form_schema.csv', index=False)

    #
    # Denormalize Form Results
    #
    full_table_data: list = load_pickle_list_from_s3(s3_client, 'form_results')
    form_results_df = denormalize_form_results(full_table_data)
    questions_df = denormalize_form_questions(full_table_data)

    tmp_df = form_results_df \
        .merge(questions_df, on='form_result_id')

    tmp_df = tmp_df[ColumnOrder.FORM_RESULTS]

    # Limit to Custom Forms that are in Form Results and only Custom Forms in Form Results that exist
    custom_form_ids = custom_form_schema_df['custom_form_id'].unique().tolist()
    tmp_df = tmp_df.loc[tmp_df['custom_form_id'].isin(custom_form_ids)]

    #
    # Join Schema to Form Results
    #
    df = tmp_df.merge(
        custom_form_schema_df,
        how='inner',
        left_on=['custom_form_id', 'question_title', 'question_answer'],
        right_on=['custom_form_id', 'question_formik_key', 'answer_value']
    )

    df[["Survey_col", "merge_id"]] = df["form_result_p_client"].str.split("$", expand=True)

    # df.to_csv('./denormalized_form_results.csv', index=False)
    merge_with_survey = df.merge(survey_df, left_on="merge_id", right_on="_id")
    #
    # Aggregate Responses per each...
    # Surveying Organization | Custom Form ID | Question ID | Answer ID
    #
    base_cols = [
                'form_result_surveying_organization',
                'custom_form_id',
                'custom_form_name',
                'custom_form_created_at',
                'custom_form_active',
                'question_id',
                'question_label',
                'question_title',
                'answer_id',
                'answer_label'
            ]
    agg_cols = base_cols + groupby_cols
    agg_df = merge_with_survey \
        .groupby(
            agg_cols,
            as_index=False
        )['answer_value'] \
        .value_counts() \
        .rename(columns={'count': 'answer_count'})
    # agg_df.to_csv('./aggregated_results.csv', index=False)

    print(agg_df.head(20))

    return agg_df


def denormalize_form_results(forms_data: list):

    # Remove nested "fields" which are denormalized elsewhere and "location" which we do not need.
    all_fields = get_fields_from_list_of_dicts(forms_data)
    cols = [
        c
        for c in all_fields
        if c not in (
            'fields',
            'location',
            'organizations',
            '_p_loopClient')
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
        .add_prefix('form_result_')

    # Final column name cleaning
    df = df.rename(columns={
        'form_result_form_specifications_id': 'custom_form_id',
        'form_result_title': 'custom_form_name',
        'form_result_description': 'custom_form_description'
    })

    return df


def denormalize_form_questions(forms_data: list):

    # Get unique nested fields across all data set
    fields_list = []
    for fields_data in forms_data:
        for cols_data in fields_data.get('fields'):
            fields_list.extend(list(cols_data.keys()))
    cols = get_unique_fields_from_list(fields_list)

    rows = []
    for form_data in forms_data:
        fields = form_data.get('fields')
        form_result_id = form_data.get('_id')

        for item in fields:
            row = [form_result_id, shortuuid_random()]

            # Exclude rows that contain duplicate top level information
            if item.get('title') not in (
                'appVersion',
                'location',
                'phoneOS',
                'surveyingOrganization',
                'surveyingUser'
            ):

                for col in cols:
                    row.append(item.get(col))
                rows.append(row)

    # Prepend column for ID
    cols_renamed = [f'question_{i}' for i in cols]
    cols_renamed.insert(0, 'form_result_id')
    cols_renamed.insert(1, 'form_result_response_id')
    cols_formatted = to_snake_case(cols_renamed)

    df = pd.DataFrame(rows, columns=cols_formatted)

    # Create new rows from Multi Select answers
    df = df.explode('question_answer')

    return df


if __name__ == '__main__':
    get_form_results_df(["age", "sex"])
