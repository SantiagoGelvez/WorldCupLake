# World Cup Data Engineering

Ingests World Cup data from [API-Football](https://www.api-football.com/) v3 into a
Databricks bronze layer, ready for silver/gold modelling downstream.

## Bronze design

Two tables, created by [`ingest/schema/bronze_ddl.sql`](ingest/schema/bronze_ddl.sql):

| Table | Role |
|---|---|
| `raw_api_responses` | Append-only log of untouched API payloads, tagged with the endpoint and fixture they came from. Never updated or deleted, so a parsing bug is fixed by replaying rows rather than re-fetching them. |
| `ingestion_checkpoint` | Work queue of `(fixture_id, endpoint)` pairs. Seeded by the bootstrap, drained by the backfill. |

The split exists because of the rate limit: the free tier allows **100 requests/day**, and a
World Cup has ~64 fixtures × 3 detail endpoints. The queue makes ingestion resumable across
days, and the append-only log makes re-fetching unnecessary once a payload has landed.

A checkpoint is `pending`, `done`, or `failed`. **`failed` is terminal** — the backfill never
retries it, so a fixture the API has no data for cannot burn the daily budget on every run.
Re-queue one by hand if it's worth another try:

```sql
UPDATE wc_project.bronze.ingestion_checkpoint
SET status = 'pending'
WHERE fixture_id = <id> AND endpoint = '<endpoint>';
```

## Setup

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env   # then fill in the API key and Databricks connection
```

Run [`ingest/schema/bronze_ddl.sql`](ingest/schema/bronze_ddl.sql) in the Databricks SQL
Editor to create the catalog, schema, and tables. If you override `DATABRICKS_CATALOG` or
`DATABRICKS_SCHEMA` in `.env`, change the DDL to match.

## Running

**Bootstrap** — fetch the season's fixtures and standings, and queue every finished fixture
for detail ingestion. Idempotent: re-running never resets in-flight progress.

```bash
python -m ingest.bootstrap [--season 2022] [--league 1]
```

**Backfill** — drain a budgeted batch of the queue, fetching statistics, lineups, and players
per fixture. Run it once a day until the backlog empties.

```bash
python -m ingest.backfill_batch [--limit 10] [--dry-run]
```

| Flag | Effect |
|---|---|
| `--limit N` | Cap requests for this run, overriding `WC_DAILY_BUDGET` |
| `--dry-run` | Log the backlog that *would* be fetched; makes no API calls and no writes |

Both commands exit `0` on success and `1` on failure, and log to stderr at `LOG_LEVEL`
(default `INFO`).

## Layout

```
ingest/
  api_client.py    API-Football client: connection reuse, retry/backoff, error detection
  bootstrap.py     Entrypoint: fixtures + standings + queue seeding
  backfill_batch.py Entrypoint: drains the queue a batch at a time
  bronze.py        All Databricks writes and checkpoint state
  config.py        Environment-backed settings, resolved explicitly at startup
  schema/          Bronze DDL
```

## Development

```bash
pip install -e '.[dev]'
ruff check ingest/
```
