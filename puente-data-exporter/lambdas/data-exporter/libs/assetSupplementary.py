import pandas as pd

from libs.utils import alter_multiselect_fields, convert_fields_to_columns

def assetSupplementary(df):
    alter_multiselect_fields(df['fields'])
    df = convert_fields_to_columns(df)
    df['surveyingOrganizationSupplementary'] = df['surveyingOrganizationSupplementary'].str.strip()
    df = df.replace({pd.np.nan: ''})
    return df