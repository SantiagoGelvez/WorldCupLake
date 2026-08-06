-- DDL bronze layer on databricks
-- Run in SQL Editor

CREATE CATALOG IF NOT EXISTS wc_project;
USE CATALOG wc_project;

CREATE SCHEMA IF NOT EXISTS wc_project.bronze;
USE SCHEMA bronze;

CREATE TABLE IF NOT EXISTS ingestion_checkpoint (
    fixture_id BIGINT,
    endpoint STRING,
    status STRING,
    attempts INT,
    last_attempt_at TIMESTAMP,
    PRIMARY KEY (fixture_id, endpoint)
);

CREATE TABLE IF NOT EXISTS raw_api_responses (
    ingested_at TIMESTAMP,
    endpoint STRING,
    fixture_id BIGINT,
    raw_payload STRING
);

-- Validate table creation
SHOW TABLES IN bronze;
