import json
import requests
import pandas as pd

import secretz

PARSE_SERVER = "https://parseapi.back4app.com/classes"

SPECIFIER = "SurveyData"

COMBINED_URL = "/".join([PARSE_SERVER, SPECIFIER])

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

def download_data():
    headers = {
            "Content-Type": "application/json",
            "X-Parse-Application-Id": secretz.APPLICATION_ID,
            "X-Parse-REST-API-Key": secretz.REST_API_KEY,
        }

    response = requests.get(
            COMBINED_URL,
            headers=headers
        )
    response.raise_for_status()

    json_obj = response.json()
    normalized = pd.json_normalize(json_obj["results"])

    return normalized

def update_data(updated_data):
    headers = {
            "Content-Type": "application/json",
            "X-Parse-Application-Id": secretz.APPLICATION_ID,
            "X-Parse-REST-API-Key": secretz.REST_API_KEY,
        }
    response = requests.request(
        "POST", COMBINED_URL, headers=headers, data=updated_data.to_json(orient='split')
    )

    return json.loads(response.text)

def main():
    normalized_data = download_data()
    print(normalized_data["communityname"].unique())
    updated_data = map_community_and_city_names(normalized_data)
    print(updated_data["communityname"].unique())
    response = update_data(updated_data)
    print(response)
    new_data = download_data()
    print(new_data["communityname"].unique())

if __name__ == "__main__":
    main()


