from libs.utils import write_csv_to_s3

import pandas as pd
import numpy as np

def vitals(df, survey_org):
    #df = restCall(specifier="Vitals")

    """ALL CLEANING HERE"""

    # clean none and NaN
    # df.replace({np.nan: ''}, inplace=True)

    df.replace({",": np.nan}, inplace=True)

    # drop duplicates from columns
    duplicate_subset = [
        "height",
        "weight",
        "respRate",
        "bmi",
        "bloodPressure",
        "bloodSugar",
        "bloodOxygen",
        "temp",
        "pulse",
        "hemoglobinLevels",
        "painLevels",
    ]

    df.drop_duplicates(subset=duplicate_subset, inplace=True)
    

    # filter columns
    columns = [
        "objectId",
        "client.objectId",
        "createdAt",
        "updatedAt",
        "height",
        "weight",
        "respRate",
        "bmi",
        "bloodPressure",
        "bloodSugar",
        "bloodOxygen",
        "temp",
        "pulse",
        "hemoglobinLevels",
        "painLevels",
        "surveyingUser",
    ]

    df = df[columns]

    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    df["bloodSugar"] = df["bloodSugar"].str.strip("+")



    # print(df.shape)
    # print(df.isna().sum())

    # print(df.dtypes)

    # #print(df["bloodPressure"].value_counts())

    # print(df.loc[df["bloodSugar"].str.len()>3, "bloodSugar"].value_counts())
    # print(df.loc[df["bloodSugar"].replace({"":np.nan, " ":np.nan}).astype(float) > 200, "bloodSugar"].count())

    """split systolic and diastolic"""
    # replace some words/characters using regex
    df["bloodPressure"] = df["bloodPressure"].replace(
        {
            "\s*over\s*": "/",
            "\s*Over\s*": "/",
            "mm(/)*Hg": "",
            " ": "",
            "When|when": "",
            "-": "/",
        },
        regex=True,
    )
    # strip whitespace
    df["bloodPressure"] = df["bloodPressure"].str.strip()
    new = df["bloodPressure"].str.split("/", n=1, expand=True)

    # new data frame with split value columns

    # making seperate first name column from new data frame
    df["Systolic"] = new[0]

    # making seperate last name column from new data frame
    df["Diastolic"] = new[1]

    #a few bloodPressures are len 5 with no slash, I think its the first three are systolic and last 2 are diastolic
    df.loc[~(df["bloodPressure"].fillna("").str.contains("/"))&(df["bloodPressure"].str.len()==5), "Systolic"] = df["bloodPressure"].str[0:2]
    df.loc[~(df["bloodPressure"].fillna("").str.contains("/"))&(df["bloodPressure"].str.len()==5), "Diastolic"] = df["bloodPressure"].str[3:5]


    # rename columns
    df.rename(
        columns={
            "objectId": "vitalId",
            "client.objectId": "objectId",
            "surveyingUser": "surveyingUserSupplementary",
        },
        inplace=True,
    )

    # columns to turn into floats instead of strings
    float_cols = [
        "height",
        "weight",
        "respRate",
        "bmi",
        "bloodSugar",
        "bloodOxygen",
        "temp",
        "pulse",
        "hemoglobinLevels",
        "Systolic",
        "Diastolic",
    ]

    #print(df.loc[~(df["bloodPressure"].isna())&(df["Diastolic"].isna()), ["bloodPressure", "Diastolic", "Systolic"]])

    # replace commas and any non-digits (using regex)
    df[float_cols] = df[float_cols].replace({",": "", "\D+": ""}, regex=True)
    # turn columns into floats
    df[float_cols] = df[float_cols].apply(pd.to_numeric)

    #print(df.isna().sum())

    specifier = "Vitals"
    key = 'clients/'+survey_org+'/data/'+specifier+'/'+specifier+'.csv'
    url = write_csv_to_s3(df, key)


    # for col in ["bloodPressure"]:
    #    print(col)
    #    print(*df[col].unique())

    return {"message": "Main Records Success :)", "data": df.to_json(), "url": url}