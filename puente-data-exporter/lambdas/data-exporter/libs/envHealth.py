from libs.utils import write_csv_to_s3
import numpy as np
import pandas as pd
import os

import boto3

def envHealth(df, survey_org):

    #df = restCall(specifier="HistoryEnvironmentalHealth", survey_org=survey_org)

    """
    PUT ALL CLEANING HERE
    """
    # replace nan
    df.replace({np.nan: ""}, inplace=True)

    # drop duplicates
    # TO CHANGE: right now im manually specifying these columns but idk where they came from
    duplicate_subset = [
        "yearsLivedinthecommunity",
        "yearsLivedinThisHouse",
        "waterAccess",
        "typeofWaterdoyoudrink",
        "bathroomAccess",
        "latrineAccess",
        "clinicAccess",
        "conditionoFloorinyourhouse",
        "conditionoRoofinyourhouse",
        "medicalproblemswheredoyougo",
        "dentalproblemswheredoyougo",
        "biggestproblemofcommunity",
        "timesperweektrashcollected",
        "wheretrashleftbetweenpickups",
        "numberofIndividualsLivingintheHouse",
        "numberofChildrenLivinginHouseUndertheAgeof5",
        "houseownership",
        "stoveType",
        "govAssistance",
        "foodSecurity",
        "electricityAccess",
        "houseMaterial",
    ]

    df.drop_duplicates(subset=duplicate_subset, inplace=True)

    # filter columns
    filter_columns = [
        "objectId",
        "client.objectId",
        "createdAt",
        "updatedAt",
        "yearsLivedinthecommunity",
        "yearsLivedinThisHouse",
        "waterAccess",
        "typeofWaterdoyoudrink",
        "bathroomAccess",
        "latrineAccess",
        "clinicAccess",
        "conditionoFloorinyourhouse",
        "conditionoRoofinyourhouse",
        "medicalproblemswheredoyougo",
        "dentalproblemswheredoyougo",
        "biggestproblemofcommunity",
        "timesperweektrashcollected",
        "wheretrashleftbetweenpickups",
        "numberofIndividualsLivingintheHouse",
        "numberofChildrenLivinginHouseUndertheAgeof5",
        "houseownership",
        "stoveType",
        "govAssistance",
        "foodSecurity",
        "electricityAccess",
        "houseMaterial",
        "surveyingUser",
    ]

    df = df[filter_columns]

    # water access
    water_access_replace_dict = {
        "1AWeek": "1x / Week",
        "2-3AWeek": "2-3x / Week",
        "everyday": "Every Day",
        "4-6AWeek": "4-6x / Week",
        "1AMonth": "1x / Month",
        "N/A": "N/A",
        "": "N/A",
    }
    df["waterAccess"].replace(water_access_replace_dict, inplace=True)

    # water type
    water_type_replace_dict = {
        "bottled": "Bottled/Filtered",
        "filtered": "Bottled/Filtered",
        "tap": "Tap/Unfiltered",
        "N/A": "N/A",
        "": "N/A",
    }

    df["typeofWaterdoyoudrink"].replace(water_type_replace_dict, inplace=True)

    # number of individuals

    # bathroom+latrine+clinic access
    # all have the same unique values, so same fcn
    yes_no_cols = ["bathroomAccess", "latrineAccess", "clinicAccess"]
    yes_no_replace_dict = {"": "N/A", "Y": "Yes", "N": "No"}
    df[yes_no_cols] = df[yes_no_cols].replace(yes_no_replace_dict)

    # condition of floor
    floor_replace_dict = {
        "cementWorking": "Cement/Functional",
        "dirtPoor": "Dirt/Bad",
        "cementPoor": "Cement/Bad",
        "dirtWorking": "Dirt/Functional",
        "": "N/A",
    }
    df["conditionoFloorinyourhouse"].replace(floor_replace_dict, inplace=True)

    # condition of roof
    roof_replace_dict = {
        "normal": "Functional",
        "working": "Functional",
        "bad": "Needs Repair",
        "poor": "Needs Repair",
        "": "N/A",
    }
    df["conditionoRoofinyourhouse"].replace(roof_replace_dict, inplace=True)

    # rename columns
    rename_dict = {
        "objectId": "envId",
        "client.objectId": "objectId",
        "surveyingUser": "surveyingUserSupplementary",
    }

    df.rename(columns=rename_dict, inplace=True)

    # for col in ['conditionoFloorinyourhouse','conditionoRoofinyourhouse', 'numberofIndividualsLivingintheHouse']:
    #    print(col)
    #    print(df[col].unique())

    # key = f"envHealth_{survey_org}.csv"
    specifier = "HistoryEnvironmentalHealth"
    key = 'clients/'+survey_org+'/data/'+specifier+'/'+specifier+'.csv'
    url = write_csv_to_s3(df, key)
    #writing to csv in s3
    # s3 = boto3.resource('s3')
    # bucket = s3.Bucket(BUCKET_NAME)

    # tmp_path = "/tmp/"
    # org_path = f"{survey_org}"
    # out_name = "envHealth.csv"

    # temp_file = os.path.join(tmp_path, org_path, out_name)
    # key = os.path.join(org_path, out_name)

    # df.to_csv(temp_file)

    # bucket.upload_file(temp_file, key)

    return {"message":"Env Health Success :)", "data": df.to_json(), "url": url}