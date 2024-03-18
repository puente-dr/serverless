from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
import json

from shared_modules.env_utils import (
    NOSQL_TABLES,
    get_engine_str
)
from shared_modules.utils import restCall

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

def create_bronze_layer(tables_to_ingest):
    for table in tables_to_ingest:
        df = restCall(table, None)
        print(table)

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

        engine_str = get_engine_str()

        engine = create_engine(engine_str)

        table_name = f"{table.lower()}_bronze"

        if applied_json > 0:
            df.to_sql(
                table_name,
                engine,
                if_exists="replace",
                index=False,
                chunksize=1000,
                dtype={"dictionary_column": JSONB},
            )
        else:
            df.to_sql(table_name, engine, if_exists="replace", index=False)
