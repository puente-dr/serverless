import hashlib
import requests
from pandas import json_normalize, read_sql_query
import json
import psycopg2
import numpy as np

from shared_modules.env_utils import (
    APP_ID,
    MASTER_KEY,
    REST_API_KEY,
    PG_HOST,
    PG_DATABASE,
    PG_PORT,
    PG_USERNAME,
    PG_PASSWORD,
)


def query_db(query):
    conn = connection()
    df = read_sql_query(query, conn)
    conn.close()
    return df


def query_bronze_layer(table):
    query = f"""
    SELECT *
    FROM {table.lower()}_bronze
    """
    df = query_db(query)
    return df


def check_valid_field(field):
    if isinstance(field, str):
        check = "test" in field.lower()
    else:
        check = field in [np.nan, None]
    return check


def connection():
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        port=PG_PORT,
        user=PG_USERNAME,
        password=PG_PASSWORD,
    )
    return conn


def add_surveyuser_column(df):
    df["default_value"] = df["firstname"] + df["lastname"]
    df["survey_user"] = (
        df[["default_value", "username", "firstname", "lastname"]]
        .bfill(axis=1)
        .iloc[:, 0]
    )
    return df


def restCall(specifier, custom_form_id, url="https://parseapi.back4app.com/classes/"):
    """
    Parameters
    ----------
    specifier: string, required
        Parse model to query

    custom_form_id: string, not required
        Custom form object id

    url: string, required
        Parse endpoint
    Returns
    ------
    Pandas dataframe with query results

    """

    #
    # Build Request URL, Headers, and Parameters
    #

    common_params = {
        "order": "-updatedAt",
        "limit": 500000,
    }
    if specifier == "users":
        split_url = url.split("/")
        url = "/".join(split_url[: len(split_url) - 2]) + "/"

    combined_url = url + specifier

    headers = {
        "Content-Type": "application/json",
        "X-Parse-Application-Id": APP_ID,
        "X-Parse-Master-Key": MASTER_KEY,
        "X-Parse-REST-API-Key": REST_API_KEY,
    }

    params = dict()

    params = {
        **common_params,
    }

    response = requests.get(combined_url, params=params, headers=headers)
    response.raise_for_status()

    json_obj = response.json()

    # normalize (ie flatten) data, turns it into a pandas df
    normalized = json_normalize(json_obj["results"])

    return normalized


def to_camel_case(text):
    s = text.replace("-", " ").replace("_", " ")
    s = s.split()
    if len(text) == 0:
        return text
    return s[0] + "".join(i.capitalize() for i in s[1:])


def md5_encode(s):
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def parse_json_config(json_path):
    with open(json_path, "r") as j:
        questions = json.loads(j.read())
        question_values_list = []
        for question in questions:
            formikkey = question["formikKey"]
            label = question["label"]
            uuid = md5_encode(formikkey)
            field_type = question["fieldType"]
            if "options" in question.keys():
                options = [option.get("value") for option in question["options"]]
            else:
                options = None

            question_values = (uuid, label, formikkey, field_type, options)
            question_values_list.append(question_values)

    return question_values_list


def unique_combos(df, col_list):
    string_dtypes = df.select_dtypes(include=[object]).columns.tolist()
    strings_in_col_list = [col for col in col_list if col in string_dtypes]
    df[strings_in_col_list] = df[strings_in_col_list].apply(lambda x: x.str.strip())
    unique_combos = df.groupby(col_list, dropna=False).size().reset_index()[col_list]
    return unique_combos


def coalesce_pkey(val_df, pkey):
    return val_df.groupby(pkey).first().reset_index()


def get_subquestions(d):
    ans = d.get("answer")

    if isinstance(ans, dict):
        return [{"title": title, "answer": answer} for title, answer in ans.items()]
    else:
        return [d]
