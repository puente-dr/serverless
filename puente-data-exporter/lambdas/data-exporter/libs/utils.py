import pandas as pd
import numpy as np
import re
import distance
from datetime import date

import sys
import os
import boto3
import io

sys.path.append(os.path.join(os.path.dirname(__file__)))

import secretz

def write_csv_to_s3(df, key):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=secretz.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=secretz.AWS_SECRET_ACCESS_KEY
        )

    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        response = s3_client.put_object(
            Bucket=secretz.AWS_S3_BUCKET,
            Key=key,
            Body=csv_buffer.getvalue()
        )

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    
    if status == 200:
        print(f"Successful S3 put_object response. Status - {status}")
    else:
        print(f"Unsuccessful S3 put_object response. Status - {status}")

    url = f"s3://{secretz.AWS_S3_BUCKET}/{key}"
    return url

def calculate_age(born, age):
    """
    if (born[:3] == "0/0" or born[:2]=="00"):
        pass
    elif "-" in born:
        born = datetime.strptime(born, "%Y-%m-%d").date()
        today = date.today()
        return today.year - born.year - ((today.month, today.day) < (born.month, born.day))
    elif "/" in born:
        mod = born.split("/")
        if (int(mod[0]) > 30):
            pass
        else:
            born = datetime.strptime(born, "%d/%m/%Y").date()
            today = date.today()
            return today.year - born.year - ((today.month, today.day) < (born.month, born.day))
    """
    if age == "":
        if "-" in born:
            mod = born.split("-")
            # born = datetime.strptime(born, "%Y-%m-%d").date()
            today = date.today()
            return int(today.year - int(mod[0]))
        elif "/" in born:
            """
            x = ""
            for s in born.split():
                if s.isdigit():
                    x+=s
            """
            mod = born.split("/")
            today = date.today()
            try:
                return int(today.year - int(mod[2]))
            except:
                return None
        else:
            return None
    else:
        return age

def split(delimiters, string, maxsplit=0):
    regexPattern = "|".join(map(re.escape, delimiters))
    return re.split(regexPattern, string, maxsplit)


def update_comm_cities_provinces():
    sys.path.append(os.path.join(os.path.dirname(__file__)))

    # read in all correct community/city/province combos
    all_data = pd.read_csv("puente-data-exporter/lambdas/data-exporter/data/Communities_Cities_Provinces.csv", encoding="latin1")
    # only where city exists
    all_data_filtered = all_data.loc[~all_data["City"].isnull()]

    # unique values only
    communities = all_data_filtered["Communities"].unique()
    cities = all_data_filtered["City"].unique()
    provinces = all_data_filtered["Province"].unique()

    return communities, cities, provinces


def fix_typos(df, col1, col2, col3):

    # get correct community/city/province to compare against
    correct_communities, correct_cities, correct_provinces = (
        update_comm_cities_provinces()
    )

    # make dict of them
    correct_dict = {
        "communityname": correct_communities,
        "city": correct_cities,
        "province": correct_provinces,
    }

    # lower case, strip white space, remove a few characters
    df[[col1, col2, col3]] = df[[col1, col2, col3]].apply(lambda x: x.str.lower())
    df[[col1, col2, col3]] = df[[col1, col2, col3]].apply(lambda x: x.str.strip())
    df[[col1, col2, col3]] = df[[col1, col2, col3]].replace(
        {".": "", ",": "", "-": "", "/": ""}
    )

    # get similarity between two strings
    def dist(str1, str2):
        return distance.levenshtein(str1, str2)

    def closest(col, correct_col):
        # get the true lists for the column
        correct_list = correct_dict[correct_col]

        # a few common mispellings that get missed by dist<=3
        spm_mispellings = [
            "spm",
            "spn",
            "macori",
            "macoris",
            "s p m",
            "sp.",
            "pedro d",
            "smp",
            "sampedro",
            "san pefro ",
            "san pedro f",
            "samoedro",
        ]
        constanza_mispellings = ["comstanza", "const", "con", "cosntanza"]

        spm = "San Pedro de Macoris"
        constanza = "Constanza"

        col_val_dict = {}
        mismatches = {}
        # for each unique value in the column
        for col_val in np.unique(col):

            # things that clearly aren't community/city/province names
            if (col_val in ["", " ", "N/A"]) | (len(col_val) <= 2):
                col_val_dict[col_val] = "Other/NA"

            # san pedro de macoris alternatives
            elif any([val in col_val for val in spm_mispellings]) & (
                spm in correct_list
            ):
                col_val_dict[col_val] = spm

            # constanza alternatives
            elif any([val in col_val for val in constanza_mispellings]) & (
                constanza in correct_list
            ):
                col_val_dict[col_val] = constanza
            else:
                dist_list = [
                    dist(col_val.lower(), correct)
                    for correct in [c.lower() for c in correct_list]
                ]
                # index of distance list that has the minimum distance
                min_idx = np.argmin(dist_list)
                # get the correct value with the above index
                closest_correct = correct_list[min_idx]
                # if distance is <=3, use the correct value to replace
                if dist_list[min_idx] <= 3:
                    col_val_dict[col_val] = closest_correct
                # otherwise it is "Other/NA"
                else:
                    mismatches[col_val] = closest_correct
                    col_val_dict[col_val] = "Other/NA"

        # show which values didn't match with anything and what their closest match was
        # use this to debug further, make the matching better
        # print("col: ", col)
        # print("mistmatches: ", mismatches)
        replaced_col = col.replace(col_val_dict)

        return replaced_col

    # clean1, clean2, clean3=col1+"_clean",col2+"_clean",col3+"_clean"
    # df[[clean1,clean2,clean3]]=df[[col1,col2,col3]].apply(lambda x: closest(x, x.name))
    df[[col1, col2, col3]] = df[[col1, col2, col3]].apply(lambda x: closest(x, x.name))

    return df

def alter_multiselect_fields(fields):
    for index in fields.index:
        new_values = []
        for field in fields.loc[index]:
            if 'answer' in field.keys() and isinstance(field['answer'], list):
                for answer in field['answer']:
                    new_values.append({
                        'title': field['title'] + '_' + answer,
                        'answer' : 'Yes'
                    })
            else:
                new_values.append(field)
        fields.loc[index] = new_values
