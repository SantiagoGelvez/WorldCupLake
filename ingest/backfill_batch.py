"""Drain a budgeted batch of the per-fixture bronze work queue.

Reads pending (fixture, endpoint) pairs seeded by the bootstrap, fetches each one from
API-Football, and appends the payload to the raw bronze table. The batch is capped by
WC_DAILY_BUDGET because the free tier allows only 100 requests/day.

Usage:
    python -m ingest.backfill_batch [--limit 10] [--dry-run]
"""

import argparse
import logging
import sys
import time

import requests

from ingest.api_client import BACKFILL_ENDPOINT_PATH, ApiFootballClient, ApiFootballError
from ingest.bronze import (
    CheckpointStatus,
    connect,
    get_pending_backfill,
    mark_checkpoint,
    save_raw_response,
)
from ingest.config import Settings, configure_logging, load_settings

log = logging.getLogger(__name__)

# Spacing between calls, so a batch trickles out instead of arriving as a burst the
# provider's rate limiter would rather reject.
REQUEST_INTERVAL_SECONDS = 1


def backfill_batch(settings: Settings, limit: int | None = None,
                   dry_run: bool = False) -> tuple[int, int]:
    """Fetch and store up to ``limit`` pending fixtures; return (done, failed) counts.

    Defaults to ``settings.daily_budget`` pairs per run. Each pair is stored and marked
    independently, so one fixture the API has no data for cannot abort the batch -- it is
    marked failed, which is terminal, and the run moves on to the next pair.
    """
    budget = settings.daily_budget if limit is None else limit
    log.info('Starting World Cup backfill (budget: %d requests)', budget)

    # The warehouse is needed either way; the API client only for a real run, so a dry run
    # never builds a session around the key.
    with connect(settings) as con, con.cursor() as cursor:
        pending = get_pending_backfill(cursor, settings, limit=budget)

        if dry_run:
            for fixture_id, endpoint, status in pending:
                log.info('[dry-run] Would fetch fixture %s - %s (%s)', fixture_id, endpoint, status)
            log.info('[dry-run] %d pair(s) selected, no API calls or writes made', len(pending))
            return 0, 0

        done, failed = 0, 0
        with ApiFootballClient(settings.api_key) as api:
            for i, (fixture_id, endpoint, _) in enumerate(pending):
                if i:
                    time.sleep(REQUEST_INTERVAL_SECONDS)

                query = {'fixture': fixture_id}
                try:
                    payload = api.get(BACKFILL_ENDPOINT_PATH[endpoint], **query)
                # ApiFootballError covers a 200 response whose payload reports an error,
                # which is how the API says it has no data for this fixture;
                # RequestException covers network failures the retry policy outlived.
                except (ApiFootballError, requests.RequestException) as exc:
                    log.error('Backfill failed for fixture %s (%s): %s',
                              fixture_id, endpoint, exc)
                    mark_checkpoint(cursor, settings, fixture_id=fixture_id,
                                    status=CheckpointStatus.FAILED, endpoint=endpoint)
                    failed += 1
                    continue

                save_raw_response(cursor, settings, endpoint, payload, fixture_id=fixture_id)
                mark_checkpoint(cursor, settings, fixture_id=fixture_id,
                                status=CheckpointStatus.DONE, endpoint=endpoint)
                done += 1

    log.info('Backfill finished: %d done, %d failed, %d requested', done, failed, len(pending))
    return done, failed


def _positive_int(value: str) -> int:
    """argparse type for --limit: a zero or negative LIMIT is a confusing SQL error."""
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError('must be 1 or greater')
    return number


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--limit', type=_positive_int,
                        help='Max requests this run (overrides WC_DAILY_BUDGET)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Log the selected backlog without calling the API or writing')
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    configure_logging()
    try:
        settings = load_settings()
        backfill_batch(settings, limit=args.limit, dry_run=args.dry_run)
    # RuntimeError covers both missing configuration and ApiFootballError raised outside the
    # per-fixture loop; RequestException covers network failures the retry policy outlived.
    except (RuntimeError, requests.RequestException) as exc:
        log.error('Backfill failed: %s', exc)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
