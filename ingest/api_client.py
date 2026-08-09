"""Thin client for the API-Football v3 REST API."""

import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

API_HOST = 'v3.football.api-sports.io'
BASE_URL = f'https://{API_HOST}'

# Fixture status codes that mean the match is over and its detail endpoints are worth ingesting.
FINISHED_STATUSES = frozenset({'FT', 'PEN', 'AET'})
BACKFILL_ENDPOINT_PATH = {
    "statistics": "/fixtures/statistics",
    "lineups": "/fixtures/lineups",
    "players": "/fixtures/players"
}


class ApiFootballError(RuntimeError):
    """The API answered, but the payload reports an error instead of data."""


class ApiFootballClient:
    """Reuses one HTTP connection and retries transient failures.

    The free tier is rate limited, so 429 responses are retried with backoff and the
    server's ``Retry-After`` header is honoured.
    """

    def __init__(self, api_key: str, timeout: int = 30, base_url: str = BASE_URL):
        self._timeout = timeout
        self._base_url = base_url
        self._session = requests.Session()
        self._session.headers.update({'x-apisports-key': api_key})
        retry = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=('GET',),
            respect_retry_after_header=True,
        )
        self._session.mount('https://', HTTPAdapter(max_retries=retry))

    def get(self, endpoint: str, **params) -> dict:
        """Fetch one endpoint and return its decoded payload.

        Raises ApiFootballError when the payload carries errors: API-Football answers
        400-class problems such as a bad key or an unknown league with HTTP 200 and a
        populated ``errors`` field, which raise_for_status() cannot see.
        """
        log.info('Requesting /%s', endpoint)
        response = self._session.get(
            url=f'{self._base_url}/{endpoint}',
            params=params,
            timeout=self._timeout,
        )
        response.raise_for_status()
        payload = response.json()

        # `errors` is [] on success but a populated dict on failure.
        if payload.get('errors'):
            raise ApiFootballError(f"API returned errors on '{endpoint}': {payload['errors']}")

        log.info(
            "Received %s results from '%s' (%s daily API calls remaining)",
            payload.get('results'),
            endpoint,
            response.headers.get('x-ratelimit-requests-remaining', 'unknown'),
        )
        return payload

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> 'ApiFootballClient':
        return self

    def __exit__(self, *_exc_info) -> None:
        self.close()
