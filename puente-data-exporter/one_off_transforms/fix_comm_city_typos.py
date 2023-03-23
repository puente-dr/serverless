import json
import requests
import pandas as pd
import boto3
import io
import http.client
import time

import secretz

PARSE_SERVER_DOMAIN = "parseapi.back4app.com"
PARSE_SERVER = f"https://{PARSE_SERVER_DOMAIN}/classes"

SPECIFIER = "SurveyData"

COMBINED_URL = "/".join([PARSE_SERVER, SPECIFIER])

s3_client = boto3.client(
    "s3",
    aws_access_key_id=secretz.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=secretz.AWS_SECRET_ACCESS_KEY,
)

S3_BUCKET_NAME = "puente-public-assets"

def check_changed_values(df, check_cols):
    #print("checked cols")
    #print(df.columns)
    #check_cols = [col for col in df.columns if col not in ["communityname", "city", "objectId"]]
    #print(check_cols)
    not_equal_cols = []
    for col in check_cols:
        x = col+"_x"
        y = col+"_y"
        #print("col")
        #print(col)
        #print()
        check = df[[x, y]].copy()
        check["check"] = (check[x] == check[y])|((check[x].isna())&(check[y].isna()))
        #print("check")
        #print(check[~check["check"]])
        #print(check["check"].all())
        if not check["check"].all():
            not_equal_cols.append(col)
    return not_equal_cols

def update_data(df):
    #print(df.columns)
    connection = http.client.HTTPSConnection(PARSE_SERVER_DOMAIN, 443)
    connection.connect()
    # connection.request('PUT', '/parse/classes/GameScore/Ed1nuqPvcm', json.dumps({
    #     "score": 73453
    # }), {
    #     "X-Parse-Application-Id": "${APPLICATION_ID}",
    #     "X-Parse-REST-API-Key": "${REST_API_KEY}",
    #     "Content-Type": "application/json"
    # })
    new_commcity_df = df[(df["communityname"]!=df["new_communityname"])|(df["city"]!=df["new_city"])|(df["surveyingOrganization"]!=df["new_surveyingOrganization"])]
    print("new commcity")
    print(new_commcity_df.shape)
    for i in range(10, new_commcity_df.shape[0], 10):
        print("i ", i)
        chunk = new_commcity_df.iloc[i-10:min(i, df.shape[0])]
        requests_list = []
        for i, row in chunk.iterrows():
            object_id = row["objectId"]
            new_comm = row["new_communityname"]
            new_city = row["new_city"]
            new_surveyorg = row['new_surveyingOrganization']
            old_comm = row["communityname"]
            #print(old_comm, new_comm)
            request_dict = {
            "method": "PUT",
            "path": f"/parse/classes/SurveyData/{object_id}",
            "body": {
                "communityname": new_comm,
                "city": new_city,
                "surveyingOrganization": new_surveyorg
            }
            }
            requests_list.append(request_dict)
        connection.request('POST', '/parse/batch', json.dumps({
        "requests": requests_list
    }), {
        "X-Parse-Application-Id": f"{secretz.APPLICATION_ID}",
        "X-Parse-REST-API-Key": f"{secretz.REST_API_KEY}",
        "Content-Type": "application/json"
    })
        result = json.loads(connection.getresponse().read())
        if isinstance(result, dict):
            print(result)
            print(chunk)
        #if i% == 0:
        time.sleep(1)
    #return result


def load_dataframe_from_s3(s3_client, file_name: str, sheet_name):

    response = s3_client.get_object(
        Bucket=S3_BUCKET_NAME, Key="data/"+file_name
    )

    body = response["Body"].read()

    clean_col_df = pd.read_excel(io.BytesIO(body), sheet_name=sheet_name)
    return clean_col_df

    # pickled_df_buffer = io.BytesIO(response["Body"].read())
    # df = pd.read_pickle(pickled_df_buffer)

    # return df


def map_community_and_city_names(df, col_dict):
    # col_dict = {
    #     "communityname": "All Communities",
    #     "city": "All Cities"
    #      }
    for col, clean_col_df in col_dict.items():
        # clean_col_df = load_dataframe_from_s3(s3_client, "clean_city_and_community_names.xlsx", sheet_name)
        # print("cols")
        # print(clean_col_df.columns)
        # print(df.columns)
        # clean_col_df.columns = []
        # clean_col_df = pd.read_excel("../data/Clean City and Community Names.xlsx", sheet_name=sheet_name)
        clean_col_df["Original"] = clean_col_df["Original"].str.lower(
        ).str.replace(" ", "")
        col_map = clean_col_df.set_index("Original")["Clean"].to_dict()
        # print("col map")
        # print(col_map.keys())
        new_col = f"new_{col}"
        stripped_col = f"stripped_{col}"
        df[stripped_col] = df[col].str.lower().str.replace(" ", "")
        df[new_col] = df[stripped_col].replace(col_map)
    df = df[(pd.notnull(df["new_city"]))&(pd.notnull(df["new_communityname"]))&(pd.notnull(df["new_surveyingOrganization"]))]
    return df


def download_data(i):
    headers = {
        "Content-Type": "application/json",
        "X-Parse-Application-Id": secretz.APPLICATION_ID,
        "X-Parse-REST-API-Key": secretz.REST_API_KEY,
    }

    params = {
        "limit": 10000,
        "skip": i * 10000
    }

    response = requests.get(
        COMBINED_URL,
        params=params,
        headers=headers
    )
    response.raise_for_status()

    json_obj = response.json()
    normalized = pd.json_normalize(json_obj["results"])

    return normalized

# def update_data(updated_data):
#     headers = {
#             "Content-Type": "application/json",
#             "X-Parse-Application-Id": secretz.APPLICATION_ID,
#             "X-Parse-REST-API-Key": secretz.REST_API_KEY,
#         }
#     response = requests.request(
#         "PUT", COMBINED_URL, headers=headers, data=updated_data.to_json(orient='split')
#     )

#     return json.loads(response.text)


def main():
    all_comm_names = []
    comm_mapping = load_dataframe_from_s3(
        s3_client, "clean_city_and_community_names.xlsx", "All Communities")
    city_mapping = load_dataframe_from_s3(
        s3_client, "clean_city_and_community_names.xlsx", "All Cities")
    surveyorg_mapping = load_dataframe_from_s3(
        s3_client, "clean_surveyorgs_v2.xlsx", "clean_surveyorgs")
    col_dict = {
        "communityname": comm_mapping,
        "city": city_mapping,
        "surveyingOrganization": surveyorg_mapping
    }

    for i in range(100):
        print('i', i)
        normalized_data = download_data(i)
        #print(normalized_data.columns)
        #print(normalized_data.shape[0])
        if normalized_data.shape[0] == 0:
            break
        
        #print("pred update commnames")
        #print(len(normalized_data["communityname"].unique()))
        updated_data = map_community_and_city_names(normalized_data, col_dict)
        #print("post update")
        #print(len(updated_data["new_communityname"].unique()))
        update_data(updated_data)
    #     print(response)
        new_data = download_data(i)
        new_and_updated = new_data.merge(updated_data, on="objectId")
        check_cols = [col for col in new_data.columns if col not in ["communityname", "city", "surveyingOrganization", "objectId"]]
        changed_cols = check_changed_values(new_and_updated, check_cols)
        print("neq cols")
        print(changed_cols)
        #comm_names = list(new_data["communityname"].unique())
        #print("download")
        #print(len(comm_names))
        #all_comm_names.append(comm_names)
    #flat_comm_names = list(set([item for sublist in all_comm_names for item in sublist]))
    #print("flat comm names")
    #print(flat_comm_names)
    #pd.DataFrame(flat_comm_names).to_csv(r"C:\Users\andyk\Documents\comm_names.csv", index=False)

if __name__ == "__main__":
    main()
