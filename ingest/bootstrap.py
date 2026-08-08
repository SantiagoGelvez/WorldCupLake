"""Bootstrap the bronze layer: fixtures, standings, and the per-fixture work queue.

Usage:
    python -m ingest.bootstrap [--season 2022] [--league 1]
"""

import argparse
import logging
import sys

import requests

from ingest.api_client import FINISHED_STATUSES, ApiFootballClient
from ingest.bronze import connect, save_raw_response, seed_checkpoints
from ingest.config import Settings, configure_logging, load_settings

log = logging.getLogger(__name__)


def bootstrap(settings: Settings) -> None:
    log.info('Starting World Cup bootstrap for season %s', settings.season)
    query = {'league': settings.league_id, 'season': settings.season}

    with ApiFootballClient(settings.api_key) as api, \
            connect(settings) as con, \
            con.cursor() as cursor:

        fixtures = api.get('fixtures', **query)
        save_raw_response(cursor, settings, 'fixtures', fixtures)

        fixture_ids = [
            f['fixture']['id']
            for f in fixtures.get('response', [])
            if f['fixture']['status']['short'] in FINISHED_STATUSES
        ]
        seed_checkpoints(cursor, settings, fixture_ids)

        standings = api.get('standings', **query)
        save_raw_response(cursor, settings, 'standings', standings)

    log.info('Bootstrap finished for season %s', settings.season)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--season', type=int, help='Override WC_SEASON')
    parser.add_argument('--league', type=int, help='Override WC_LEAGUE_ID')
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    configure_logging()
    try:
        settings = load_settings(season=args.season, league_id=args.league)
        bootstrap(settings)
    # RuntimeError covers both missing configuration and ApiFootballError; RequestException
    # covers network failures that outlived the retry policy.
    except (RuntimeError, requests.RequestException) as exc:
        log.error('Bootstrap failed: %s', exc)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
