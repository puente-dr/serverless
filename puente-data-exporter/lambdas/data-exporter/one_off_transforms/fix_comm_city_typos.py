import json
import requests
import pandas as pd
import sys
import os

import secretz

PARSE_SERVER = "https://parseapi.back4app.com/classes"

SPECIFIER = "SurveyData"

def map_community_and_city_names(df):
    col_dict = {
        "communityname": "All Communities",
        "city": "All Cities"
         }
    for col, sheet_name in col_dict.items():
        clean_col_df = pd.read_excel("../data/Clean City and Community Names.xlsx", sheet_name=sheet_name)
        col_map = clean_col_df.set_index("Original")["Clean"].to_dict()
        df[col] = df[col].replace(col_map)
    return df

combined_url = "/".join([PARSE_SERVER, SPECIFIER])

headers = {
        "Content-Type": "application/json",
        "X-Parse-Application-Id": secretz.APPLICATION_ID,
        "X-Parse-REST-API-Key": secretz.REST_API_KEY,
    }

response = requests.get(
        combined_url,
        headers=headers
    )
response.raise_for_status()

json_obj = response.json()
normalized = pd.json_normalize(json_obj["results"])

print(normalized["communityname"].value_counts())

updated_df = map_community_and_city_names(normalized)

print(updated_df["communityname"].value_counts())
