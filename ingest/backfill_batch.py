"""Backfill batch
"""

import argparse
import logging
import sys
import time

import requests

from ingest.api_client import ApiFootballClient, BACKFILL_ENDPOINT_PATH
from ingest.bronze import connect, get_pending_backfill, save_raw_response, mark_checkpoint
from ingest.config import Settings, load_settings

log = logging.getLogger(__name__)


def backfill_batch(settings: Settings) -> None:
    log.info('Starting World Cup backfill')
    
    with ApiFootballClient(settings.api_key) as api, \
            connect(settings=settings) as con, \
            con.cursor() as cursor:
        pending_backfill = get_pending_backfill(cursor=cursor, settings=settings)
        done, failed = 0, 0
        for fixture_id, endpoint in pending_backfill:
            query = {'fixture': fixture_id}
            try:
                fixture = api.get(endpoint=BACKFILL_ENDPOINT_PATH[endpoint], **query)

                save_raw_response(cursor=cursor, settings=settings, endpoint=endpoint,
                    payload=fixture, fixture_id=fixture_id)
                mark_checkpoint(cursor, settings=settings, fixture_id=fixture_id,
                                status='done', endpoint=endpoint)
                done += 1
                time.sleep(0.5)
            except requests.RequestException as e:
                log.error('[Backfill] Error')
                mark_checkpoint(cursor, settings=settings, fixture_id=fixture_id,
                                status='failed', endpoint=endpoint)
                failed += 1


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--season', type=int, help='Override WC_SEASON')
    parser.add_argument('--league', type=int, help='Override WC_LEAGUE_ID')
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        settings = load_settings(season=args.season, league_id=args.league)
        backfill_batch(settings=settings)
    except (RuntimeError, requests.RequestException) as exc:
        log.error('Backfill failed: %s', exc)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
