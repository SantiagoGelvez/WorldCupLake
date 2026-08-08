import os
import json
import logging
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from databricks import sql

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s'
)
log = logging.getLogger(__name__)

API_HOST = 'v3.football.api-sports.io'
BASE_URL = f'https://{API_HOST}'
WC_LEAGUE_ID = int(os.getenv("WC_LEAGUE_ID"))
WC_SEASON = 2022
DAILY_BUDGET = 90

def _headers():
    return {'x-apisports-key': os.getenv("API_FOOTBALL_KEY")}

def _db_connection():
    log.info("Connecting to Databricks warehouse")
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )

def _save_bronze(con, endpoint: str, payload: dict, fixture_id: int = None):
    raw_payload = json.dumps(payload)
    log.info(
        "Saving raw '%s' payload to bronze",
        endpoint
    )
    con.cursor().execute(
        '''INSERT INTO wc_project.bronze.raw_api_responses(ingested_at, endpoint, fixture_id, raw_payload) VALUES(?, ?, ?, ?)''',
        [datetime.now(timezone.utc), endpoint, fixture_id, raw_payload]
    )


def bootstrap_fixtures_and_standings():
    log.info("Starting World Cup bootstrap for season %s", WC_SEASON)
    con = _db_connection()
    endpoint = 'fixtures'

    log.info("Requesting /%s", endpoint)
    fixture_resp = requests.get(
        url=f'{BASE_URL}/{endpoint}',
        headers=_headers(),
        params={
            "league": WC_LEAGUE_ID,
            "season": WC_SEASON,
        },
        timeout=30
    )
    fixture_resp.raise_for_status()
    fixture_data = fixture_resp.json()

    if fixture_data.get('errors'):
        log.warning("API returned errors on '%s': %s", endpoint, fixture_data['errors'])
    log.info(
        "Received %s fixtures (%s daily API calls remaining)",
        fixture_data.get('results'),
        fixture_resp.headers.get('x-ratelimit-requests-remaining', 'unknown')
    )

    _save_bronze(
        con=con,
        endpoint=endpoint,
        payload=fixture_data
    )

    fixture_ids = [f['fixture']['id'] for f in fixture_data.get('response', []) if f['fixture']['status']['short'] in ('FT', 'PEN', 'AET')]

    log.info("Seeding checkpoints")

    for fid in fixture_ids:
        for endpoint in ['statics', 'lineups', 'players']:
            con.cursor().execute(
                """
                MERGE INTO wc_project.bronze.ingestion_checkpoint AS t
                USING (
                    SELECT
                        ? AS fixture_id,
                        ? AS endpoint
                ) AS s
                ON t.fixture_id = s.fixture_id
                AND t.endpoint = s.endpoint
                WHEN NOT MATCHED THEN INSERT (fixture_id, endpoint, status, attempts, last_attempt_at)
                VALUES (?, ?, 'pending', 0, ?)
                """,
                [fid, endpoint, fid, endpoint, datetime.now(timezone.utc)]
            )

    log.info("Seeded checkpoints for %d fixtures", len(fixture_ids))

    endpoint = 'standings'
    log.info("Requesting /%s", endpoint)
    standings_resp = requests.get(
        url=f'{BASE_URL}/{endpoint}',
        headers=_headers(),
        params={
            "league": WC_LEAGUE_ID,
            "season": WC_SEASON,
        },
        timeout=30
    )
    standings_resp.raise_for_status()
    standings_data = standings_resp.json()

    if standings_data.get('errors'):
        log.warning("API returned errors on standings: %s", standings_data['errors'])
    log.info(
        "Received %s standings groups (%s daily API calls remaining)",
        standings_data.get('results'),
        standings_resp.headers.get('x-ratelimit-requests-remaining', 'unknown')
    )

    _save_bronze(
        con=con,
        endpoint='standings',
        payload=standings_data
    )

    con.close()
    log.info("Bootstrap finished for season %s", WC_SEASON)
