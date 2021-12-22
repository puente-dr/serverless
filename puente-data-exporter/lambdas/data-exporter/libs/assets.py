import pandas as pd

def assets(df):
    # CLEANING
    df = df.replace({pd.np.nan: ''}) #cleans new merged dataframe
    df['relatedPeople'] = df['relatedPeople'].apply(lambda x: [{"firstName":"","lastName":"","relationship":""}] if x == "" else x)

    '''
    Remove whitespace from org
    '''
    df['surveyingOrganization'] = df['surveyingOrganization'].str.strip()

    return df