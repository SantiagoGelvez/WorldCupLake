"""Writes to the bronze layer in Databricks.

The bronze layer is two tables:

``raw_api_responses``
    An append-only log of untouched API payloads, tagged with the endpoint and fixture they
    came from. Nothing is ever updated or deleted, so a payload can always be replayed into
    silver rather than re-fetched.

``ingestion_checkpoint``
    A work queue of (fixture, endpoint) pairs. The bootstrap seeds it; the backfill drains
    it a budgeted batch at a time.
"""

import json
import logging
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from enum import StrEnum

from databricks import sql
from databricks.sql.client import Connection, Cursor

from ingest.config import Settings

log = logging.getLogger(__name__)

# Per-fixture endpoints queued for later ingestion.
CHECKPOINT_ENDPOINTS = ('statistics', 'lineups', 'players')

# Rows per MERGE statement, so a large tournament cannot outgrow the statement size limit.
MERGE_CHUNK_SIZE = 150


class CheckpointStatus(StrEnum):
    """Lifecycle of one (fixture, endpoint) pair in the work queue."""

    PENDING = 'pending'
    DONE = 'done'
    FAILED = 'failed'


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


def save_raw_response(cursor: Cursor, settings: Settings, endpoint: str, payload: dict,
                      fixture_id: int | None = None) -> None:
    """Append one untouched API payload to the raw bronze table."""
    log.info("Saving raw '%s' payload to bronze", endpoint)
    cursor.execute(
        f'INSERT INTO {settings.table("raw_api_responses")}'
        ' (ingested_at, endpoint, fixture_id, raw_payload) VALUES (?, ?, ?, ?)',
        [datetime.now(UTC), endpoint, fixture_id, json.dumps(payload)]
    )


def seed_checkpoints(cursor: Cursor, settings: Settings, fixture_ids: Sequence[int],
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
                VALUES (s.fixture_id, s.endpoint, ?, 0, NULL)
            """,
            [*params, CheckpointStatus.PENDING.value]
        )
        log.info('Merged chunk %d/%d (%d pairs, %d/%d total)',
                 i, total_chunks, len(chunk), start + len(chunk), len(pairs))

    log.info('Seeded %d checkpoints across %d fixtures', len(pairs), len(fixture_ids))
    return len(pairs)


def mark_checkpoint(cursor: Cursor, settings: Settings, fixture_id: int,
                    status: CheckpointStatus, endpoint: str) -> None:
    """Record the outcome of one (fixture, endpoint) attempt and bump its attempt counter."""
    table = settings.table('ingestion_checkpoint')
    cursor.execute(
        f"""
        MERGE INTO {table} AS target
        USING (
            SELECT
                ? AS fixture_id,
                ? AS endpoint,
                ? AS status
        ) AS src
        ON target.fixture_id = src.fixture_id
        AND target.endpoint = src.endpoint
        WHEN MATCHED THEN
            UPDATE SET status = src.status,
            attempts = target.attempts + 1,
            last_attempt_at = current_timestamp()
        WHEN NOT MATCHED THEN
            INSERT (fixture_id, endpoint, status, attempts, last_attempt_at)
            VALUES (src.fixture_id, src.endpoint, src.status, 1, current_timestamp())
        """,
        [fixture_id, endpoint, status.value]
    )


def get_pending_backfill(cursor: Cursor, settings: Settings,
                         limit: int) -> list[tuple[int, str]]:
    """Claim up to ``limit`` pending (fixture, endpoint) pairs, oldest fixture first.

    Only ``pending`` rows are returned: a ``failed`` pair is terminal and is never retried
    automatically, so a fixture the API has no data for cannot burn the daily budget on
    every run. ``attempts`` is an audit counter for that decision, not an input to it --
    re-queue a pair by hand with an UPDATE to ``pending`` if it is worth another try.
    """
    table = settings.table('ingestion_checkpoint')
    cursor.execute(
        f"""
        SELECT fixture_id, endpoint
        FROM {table}
        WHERE status = ?
        ORDER BY fixture_id
        LIMIT ?
        """,
        [CheckpointStatus.PENDING.value, limit]
    )

    pending = [(int(row.fixture_id), str(row.endpoint)) for row in cursor.fetchall()]

    if not pending:
        log.info('Backlog is empty, nothing to backfill')

    return pending
