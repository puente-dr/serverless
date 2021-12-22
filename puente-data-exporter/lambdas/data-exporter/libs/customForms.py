import pandas as pd

from libs.utils import alter_multiselect_fields

def customForms(df):
    df = df.replace({pd.np.nan: ''})

    alter_multiselect_fields(df['fields'])

    df['surveyingOrganizationSuuplementary'] = df['surveyingOrganizationSupplementary'].str.strip()

    return df

    