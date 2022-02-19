from libs.utils import calculate_age

import pandas as pd


def mainRecords(df):
    # df = restCall(specifier="SurveyData", survey_org=survey_org)
    """
    Clean
    """

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
    df = df.replace({pd.np.nan: ""})

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
        "": pd.np.nan,
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
        pd.np.nan: "Other/NA",
        "puente": "Puente",
        "Rayjon": "Rayjon Share Care",
    }
    df["surveyingOrganization"] = df["surveyingOrganization"].replace(
        surv_org_replace_dict
    )

    # replace any induced nan
    df = df.replace({pd.np.nan: "N/A"})

    del df["searchIndex"]

    return df
