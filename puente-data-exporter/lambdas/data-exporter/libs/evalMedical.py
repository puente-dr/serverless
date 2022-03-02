import pandas as pd


def evalMedical(df):
    # TODO: Do we need to keep this?
    # df = restCall(specifier="EvaluationMedical", survey_org=survey_org)

    """ALL CLEANING HERE"""

    # clean none and NaN
    df = df.replace({pd.np.nan: ""})

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
    ]

    df.drop_duplicates(subset=duplicate_subset, inplace=True)

    # TODO: Is this a typo?
    df["surveyingOrganizationSuuplementary"] = df[
        "surveyingOrganizationSupplementary"
    ].str.strip()

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

    return df
