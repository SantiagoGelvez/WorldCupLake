{{ config(materialized='view') }}

with raw_statistics as (
    select
        fixture_id,
        ingested_at,
        raw_payload
    from {{ source('bronze', 'raw_api_responses') }}
    where endpoint = 'statistics'
    qualify row_number() over (partition by fixture_id order by ingested_at desc) = 1
),
json_parsed as (
    select
        fixture_id,
        from_json(
            raw_payload,
            'response ARRAY<STRUCT<
                team: STRUCT<id: INT, name: STRING>,
                statistics: ARRAY<STRUCT<type: STRING, value: STRING>>
            >>'
            ) as j
    from raw_statistics
),
explode_response as (
    select
        fixture_id,
        explode_outer(j.response) as f
    from json_parsed
),
explode_statistics as (
    select
        fixture_id,
        f.team.id as team_id,
        f.team.name as team_name,
        explode_outer(f.statistics) as stats
    from explode_response
)
select
    fixture_id,
    team_id,
    team_name,
    stats.type as stats_type,
    stats.value as stats_value
from explode_statistics