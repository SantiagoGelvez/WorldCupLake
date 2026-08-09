"""Writes to the bronze layer in Databricks."""

import json
import logging
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone

from databricks import sql
from databricks.sql.client import Connection

from ingest.config import Settings

log = logging.getLogger(__name__)

# Per-fixture endpoints queued for later ingestion.
CHECKPOINT_ENDPOINTS = ('statistics', 'lineups', 'players')

# Rows per MERGE statement, so a large tournament cannot outgrow the statement size limit.
MERGE_CHUNK_SIZE = 150

# Daily budget
DAILY_BUDGET = 3


@contextmanager
def connect(settings: Settings) -> Generator[Connection, None, None]:
    """Open a warehouse connection that always closes, including on failure."""
    log.info('Connecting to Databricks warehouse')
    con = sql.connect(
        server_hostname=settings.databricks_hostname,
        http_path=settings.databricks_http_path,
        access_token=settings.databricks_token,
    )
    try:
        yield con
    finally:
        con.close()


def save_raw_response(cursor, settings: Settings, endpoint: str, payload: dict,
                      fixture_id: int | None = None) -> None:
    """Append one untouched API payload to the raw bronze table."""
    log.info("Saving raw '%s' payload to bronze", endpoint)
    cursor.execute(
        f'INSERT INTO {settings.table("raw_api_responses")}'
        ' (ingested_at, endpoint, fixture_id, raw_payload) VALUES (?, ?, ?, ?)',
        [datetime.now(timezone.utc), endpoint, fixture_id, json.dumps(payload)]
    )


def seed_checkpoints(cursor, settings: Settings, fixture_ids: Sequence[int],
                     endpoints: Sequence[str] = CHECKPOINT_ENDPOINTS) -> int:
    """Queue every (fixture, endpoint) pair as pending work.

    Only inserts pairs that are not already tracked, so re-running the bootstrap never
    resets the progress of an in-flight ingestion. All pairs go out in a handful of
    MERGE statements rather than one per pair.
    """
    pairs = [(fid, endpoint) for fid in fixture_ids for endpoint in endpoints]
    if not pairs:
        log.info('No fixtures to seed checkpoints for')
        return 0

    table = settings.table('ingestion_checkpoint')
    total_chunks = (len(pairs) + MERGE_CHUNK_SIZE - 1) // MERGE_CHUNK_SIZE
    for i, start in enumerate(range(0, len(pairs), MERGE_CHUNK_SIZE), start=1):
        chunk = pairs[start:start + MERGE_CHUNK_SIZE]
        values = ', '.join(['(?, ?)'] * len(chunk))
        params = [value for pair in chunk for value in pair]
        cursor.execute(
            f"""
            MERGE INTO {table} AS t
            USING (SELECT * FROM (VALUES {values}) AS v(fixture_id, endpoint)) AS s
                ON t.fixture_id = s.fixture_id
               AND t.endpoint = s.endpoint
            WHEN NOT MATCHED THEN INSERT (fixture_id, endpoint, status, attempts, last_attempt_at)
                VALUES (s.fixture_id, s.endpoint, 'pending', 0, NULL)
            """,
            params
        )
        log.info('Merged chunk %d/%d (%d pairs, %d/%d total)',
                 i, total_chunks, len(chunk), start + len(chunk), len(pairs))

    log.info('Seeded %d checkpoints across %d fixtures', len(pairs), len(fixture_ids))
    return len(pairs)


def mark_checkpoint(cursor, settings: Settings, fixture_id,
                    status, endpoint):
    table = settings.table('ingestion_checkpoint')
    cursor.execute(
        f"""
        MERGE INTO {table} AS target
        USING (
            SELECT
                ? AS fixture_id,
                ? AS endpoint
        ) AS src
        ON target.fixture_id = src.fixture_id
        AND target.endpoint = src.endpoint
        WHEN MATCHED THEN
            UPDATE SET status = ?,
            attempts = target.attempts + 1,
            last_attempt_at = current_timestamp()
        WHEN NOT MATCHED THEN
            INSERT (fixture_id, endpoint, status, attempts, last_attempt_at)
            VALUES (?, ?, ?, 1, current_timestamp())
        """,
        [fixture_id, endpoint, status, fixture_id, endpoint, status]
    )


def get_pending_backfill(cursor, settings: Settings):
    table = settings.table('ingestion_checkpoint')
    cursor.execute(
        f"""
        SELECT fixture_id, endpoint
        FROM {table}
        WHERE status = 'pending'
        ORDER BY fixture_id
        LIMIT ?
        """,
        [DAILY_BUDGET]
    )

    pending = cursor.fetchall()

    if not pending:
        log.info('[Backfill] Empty backlog. Nothing for today.')

    return pending
