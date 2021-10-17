from restCall import restCall
from utils import write_csv_to_s3

import numpy as np

def evalMedical(df, survey_org):
    #df = restCall(specifier="EvaluationMedical", survey_org=survey_org)

    """ALL CLEANING HERE"""

    # clean none and NaN
    df = df.replace({np.nan: ""})

    # drop duplicate values
    duplicate_subset = [
        "chronic_condition_hypertension",
        "chronic_condition_diabetes",
        "chronic_condition_other",
        "seen_doctor",
        "received_treatment_notes",
        "received_treatment_description",
        "part_of_body",
        "part_of_body_description",
        "duration",
        "trauma_induced",
        "condition_progression",
        "notes",
        "AssessmentandEvaluation",
        "AssessmentandEvaluation_Surgical",
        "AssessmentandEvaluation_Surgical_Guess",
        "immediate_follow_up",
        "planOfAction",
        "createdAt",
    ]

    df.drop_duplicates(subset=duplicate_subset, inplace=True)

    # filter columns
    columns = [
        "objectId",
        "client.objectId",
        "createdAt",
        "updatedAt",
        "chronic_condition_hypertension",
        "chronic_condition_diabetes",
        "chronic_condition_other",
        "seen_doctor",
        "received_treatment_notes",
        "received_treatment_description",
        "part_of_body",
        "part_of_body_description",
        "duration",
        "trauma_induced",
        "condition_progression",
        "notes",
        "AssessmentandEvaluation",
        "AssessmentandEvaluation_Surgical",
        "AssessmentandEvaluation_Surgical_Guess",
        "immediate_follow_up",
        "planOfAction",
        "surveyingUser",
    ]

    df = df[columns]

    # several yes/no columns
    yes_no_cols = [
        "chronic_condition_hypertension",
        "chronic_condition_diabetes",
        "chronic_condition_other",
        "seen_doctor",
        "immediate_follow_up",
        "AssessmentandEvaluation_Surgical",
    ]
    yes_no_replace_dict = {"": "N/A", "Y": "Yes", "N": "No"}
    df[yes_no_cols] = df[yes_no_cols].replace(yes_no_replace_dict)

    # part of body
    pob_replace_dict = {
        "bones_or_joints": "Bones/Joints",
        "stomach_intestines": "Stomach/Digestive",
        "eyes": "Eyes",
        "head_mental_issue": "Brain (Mental Issue)",
        "ear_nose_throat": "Ear/Nose/Throat",
        "reproductive_organs": "Reproductive Organs",
        "bladder_urinary": "Bladder/Urinary",
        "nutrition": "Nutrition",
        "skin": "Skin",
        "": "N/A",
    }

    df["part_of_body"].replace(pob_replace_dict, inplace=True)

    # rename columns
    df.rename(
        columns={
            "objectId": "medicalEvaluationId",
            "client.objectId": "objectId",
            "surveyingUser": "surveyingUserSupplementary",
        },
        inplace=True,
    )

    key = r"evalMedical_{survey_org}.csv"
    url = write_csv_to_s3(df, key)

    # for col in ['received_treatment_notes', "received_treatment_description", "part_of_body", "part_of_body_description", 'AssessmentandEvaluation',
    #           'AssessmentandEvaluation_Surgical','AssessmentandEvaluation_Surgical_Guess']:
    #    print(col)
    #    print(df[col].unique())
    #print(df.shape)
    #print(len(df["medicalEvaluationId"].unique()))
    return {"Message": "Eval Medical Success :)", "data": df.to_json(), "url": url}