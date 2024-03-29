import json

with open("./env.json") as file:
    env = json.load(file)["AnalyticsLambdaFunctionETL"]

PG_HOST = env.get("PG_HOST")
PG_PORT = env.get("PG_PORT")
PG_DATABASE = env.get("PG_DATABASE")
PG_USERNAME = env.get("PG_USERNAME")
PG_PASSWORD = env.get("PG_PASSWORD")
APP_ID = env.get("APP_ID")
REST_API_KEY = env.get("REST_API_KEY")
MASTER_KEY = env.get("MASTER_KEY")

NOSQL_TABLES = {
    "HistoryEnvironmentalHealth": "Marketplace form for questions related to patients environment",
    "EvaluationMedical": "Marketplace form for questions related to patients current medical health",
    "Vitals": "Marketplace form for questions related to patients' vitals",
}

CONFIGS = {
    "HistoryEnvironmentalHealth": "./silver/configs/env_health_config.json",
    "EvaluationMedical": "./silver/configs/eval_medical_config.json",
    "Vitals": "./silver/configs/vitals_config.json",
}

CSV_PATH = "./Data"

def get_engine_str():
    engine_str = (
            f"postgresql://{PG_USERNAME}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
        )
    return engine_str
