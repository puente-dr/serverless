from libs.utils import fix_typos, calculate_age, write_csv_to_s3

import numpy as np
import pandas as pd
import os

import boto3


def mainRecords(df, survey_org, BUCKET_NAME):
    # df = restCall(specifier="SurveyData", survey_org=survey_org)
    """
    Clean
    """

    # filter columns
    columns = [
        "objectId",
        "fname",
        "lname",
        "nickname",
        "relationship",
        "sex",
        "dob",
        "telephoneNumber",
        "educationLevel",
        "occupation",
        "communityname",
        "city",
        "province",
        "insuranceNumber",
        "insuranceProvider",
        "clinicProvider",
        "cedulaNumber",
        "surveyingUser",
        "surveyingOrganization",
        "latitude",
        "longitude",
        "createdAt",
        "updatedAt",
        "age",
    ]

    df = df[columns]

    # remove duplicate rows
    duplicate_subset = [
        "fname",
        "lname",
        "nickname",
        "relationship",
        "sex",
        "dob",
        "telephoneNumber",
        "educationLevel",
        "occupation",
        "city",
        "province",
        "clinicProvider",
        "cedulaNumber",
        "surveyingUser",
        "surveyingOrganization",
        "latitude",
        "longitude",
        "age",
    ]

    df.drop_duplicates(subset=duplicate_subset, inplace=True)

    """ALL DATA CLEANING HERE"""

    # replace nan
    df = df.replace({np.nan: ""})

    # age calculation
    df["age"] = df.apply(lambda x: calculate_age(x["dob"], x["age"]), axis=1)

    # null relationship status set to empty string
    df["relationship"] = df["relationship"].replace("null, null", "")

    # change sex to Male/Female/NA
    sex_replace_dict = {
        "male": "Male",
        "female": "Female",
        "prefer_not_to_say": "N/A",
        "": "N/A",
    }
    df["sex"].replace(sex_replace_dict, inplace=True)

    # educationLevel
    # lessThanprimary/n --> lessThanprimary, ""-->"N/A"
    # make the formatting nicer on the other ones, matching old excel dashboard
    education_replace_dict = {
        "lessThanprimary\n": "Less Than Primary",
        "lessThanprimary": "Less Than Primary",
        "highschool": "High School",
        "primary": "Primary",
        "someCollege": "Some College",
        "college": "College",
        "someHighSchool": "Some High School",
        "": np.nan,
    }
    df["educationLevel"].replace(education_replace_dict, inplace=True)

    # community, city, province using distance metric for finding typos
    # less than 3 edits
    # print("old communities: ", df["communityname"].value_counts())
    # commented out for now took 60 seconds to ran and lambda timed out
    # df = fix_typos(df, "communityname", "city", "province")

    # print("communities: ", df["communityname"].value_counts())

    # remove whitespace from org
    df["surveyingOrganization"] = df["surveyingOrganization"].str.strip()
    # renaming
    surv_org_replace_dict = {
        "": "Other/NA",
        np.nan: "Other/NA",
        "puente": "Puente",
        "Rayjon": "Rayjon Share Care",
    }
    df["surveyingOrganization"] = df["surveyingOrganization"].replace(
        surv_org_replace_dict
    )

    # replace any induced nan
    df = df.replace({np.nan: "N/A"})

    # writing to csv in s3
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(BUCKET_NAME)

    tmp_path = "/tmp/"
    org_path = f"{survey_org}"
    out_name = "mainRecords.csv"

    temp_file = os.path.join(tmp_path, org_path, out_name)
    key = os.path.join(org_path, out_name)

    df.to_csv(temp_file)

    bucket.upload_file(temp_file, key)

    # if not os.path.exists(org_path):
    #     os.mkdir(org_path)
    # else:
    #     print("path exists?")
    # df.to_csv(test_key)

    # need bucket name, key id, secret access key, session token
    # url = write_csv_to_s3(df, key)

    # print(df.dtypes)

    # for col in ["age", "city", "province"]:
    #    print(col)
    #    print(df[col].unique())
    # use the below couple of lines to check on results of fix_typos function

    # for col in ["communityname", "city", "province"]:
    #    print(col)
    #    print(df[col].unique())

    # return {"message": "Main Records Success :)", "data": df.to_json(), "url": url}
    return {"message": "Main Records Success :)", "data": df.to_json()}
