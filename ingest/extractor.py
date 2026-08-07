import os

import requests
from dotenv import load_dotenv
from databricks import sql

load_dotenv()

API_HOST = 'v3.football.api-sports.io'
BASE_URL = f'https://{API_HOST}'
WC_LEAGUE_ID = int(os.getenv("WC_LEAGUE_ID"))
WC_SEASON = 2022
DAILY_BUDGET = 90

def _headers():
    return {'x-apisports-key': os.getenv("API_FOOTBALL_KEY")}

def _db_connection():
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )


def bootstrap_fixtures_and_standings():
    con = _db_connection()
    resp = requests.get(
        url=f'{BASE_URL}/fixtures',
        headers=_headers(),
        params={
            "league": WC_LEAGUE_ID,
            "season": WC_SEASON,
        },
        timeout=30
    )
    resp.raise_for_status()
    data = resp.json()
    with open("fixtures_response.json", "wb") as f:
        f.write(resp.content)