"""Runtime configuration, loaded from the environment.

Settings are resolved by an explicit ``load_settings()`` call rather than at import
time, so importing any module in this package never fails or reconfigures logging as
a side effect.
"""

import logging
import os
import re
from dataclasses import dataclass

from dotenv import load_dotenv

DEFAULT_CATALOG = 'wc_project'
DEFAULT_SCHEMA = 'bronze'
DEFAULT_WC_LEAGUE_ID = 1
DEFAULT_SEASON = 2022

# API-Football's free tier allows 100 requests/day. Leave headroom for the bootstrap run
# and for manual exploration rather than spending the whole allowance on one backfill.
DEFAULT_DAILY_BUDGET = 30

# Catalog/schema names are interpolated into SQL, so they must look like plain identifiers.
_IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


@dataclass(frozen=True)
class Settings:
    api_key: str
    league_id: int
    season: int
    databricks_hostname: str
    databricks_http_path: str
    databricks_token: str
    catalog: str = DEFAULT_CATALOG
    schema: str = DEFAULT_SCHEMA
    daily_budget: int = DEFAULT_DAILY_BUDGET

    def table(self, name: str) -> str:
        """Fully-qualified name for a table in the configured bronze schema."""
        return f'{self.catalog}.{self.schema}.{name}'


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        # Report the variable name only -- never its value.
        raise RuntimeError(f'Missing required environment variable: {name}')
    return value


def _require_int(name: str) -> int:
    value = _require(name)
    try:
        return int(value)
    except ValueError:
        raise RuntimeError(f'Environment variable {name} must be an integer') from None


def _identifier(name: str, default: str) -> str:
    value = os.getenv(name) or default
    if not _IDENTIFIER.match(value):
        raise RuntimeError(f'Environment variable {name} must be a plain SQL identifier')
    return value


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv('LOG_LEVEL', 'INFO').upper(),
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s'
    )
    # The connector logs every thrift round trip at INFO, which drowns out our own lines.
    logging.getLogger('databricks.sql').setLevel(logging.WARNING)


def _optional_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        raise RuntimeError(f'Environment variable {name} must be an integer') from None


def load_settings(season: int | None = None, league_id: int | None = None) -> Settings:
    """Build settings from the environment, with optional CLI overrides."""
    load_dotenv()
    return Settings(
        api_key=_require('API_FOOTBALL_KEY'),
        league_id=(league_id if league_id is not None
                   else _optional_int('WC_LEAGUE_ID', DEFAULT_WC_LEAGUE_ID)),
        season=season if season is not None else _optional_int('WC_SEASON', DEFAULT_SEASON),
        databricks_hostname=_require('DATABRICKS_SERVER_HOSTNAME'),
        databricks_http_path=_require('DATABRICKS_HTTP_PATH'),
        databricks_token=_require('DATABRICKS_TOKEN'),
        catalog=_identifier('DATABRICKS_CATALOG', DEFAULT_CATALOG),
        schema=_identifier('DATABRICKS_SCHEMA', DEFAULT_SCHEMA),
        daily_budget=_optional_int('WC_DAILY_BUDGET', DEFAULT_DAILY_BUDGET),
    )
