from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
import json

from env_utils import (
    NOSQL_TABLES,
    PG_HOST,
    PG_DATABASE,
    PG_PORT,
    PG_USERNAME,
    PG_PASSWORD,
)
from utils import restCall, connection

not_nosql_tables = ["SurveyData", "FormSpecificationsV2", "FormResults", "users"]
tables_to_ingest = list(NOSQL_TABLES.keys()) + not_nosql_tables

rename_dict = {
    "objectId": "objectIdSupplementary",
    "client.objectId": "objectId",
    "surveyingUser": "surveyingUserSupplementary",
    "surveyingOrganization": "surveyingOrganizationSupplementary",
    "createdAt": "createdAtSupplementary",
    "updatedAt": "updatedAtSuplementary",
    "yearsLivedinthecommunity": "yearsLivedinthecommunitySupplementary",
    "yearsLivedinThisHouse": "yearsLivedinThisHouseSupplementary",
    "waterAccess": "waterAccessSupplementary",
    "numberofIndividualsLivingintheHouse": "numberofIndividualsLivingintheHouseSupplementary",
}

for table in tables_to_ingest:
    df = restCall(table, None)

    for i, row in df.iterrows():
        row_arr = row.array
        for val in row_arr:
            if isinstance(val, dict):
                break

    dict_cols = ["fields"]

    applied_json = 0
    for col in dict_cols:
        if col in df.columns:
            df[col] = df[col].apply(json.dumps)
            applied_json += 1

    engine_str = (
        f"postgresql://{PG_USERNAME}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    )

    engine = create_engine(engine_str)

    table_name = f"{table.lower()}_bronze"

    if applied_json > 0:
        df.to_sql(
            table_name,
            engine,
            if_exists="replace",
            index=False,
            dtype={"dictionary_column": JSONB},
        )
    else:
        df.to_sql(table_name, engine, if_exists="replace", index=False)