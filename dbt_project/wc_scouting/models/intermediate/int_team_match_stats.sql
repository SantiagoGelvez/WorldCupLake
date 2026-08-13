{{ config(materialized='table') }}

with long_format as (
    select * from {{ ref('stg_match_statistics') }}
)
select
    fixture_id,
    team_id,
    MAX(team_name) as team_name,
    MAX(CASE WHEN stats_type = 'Shots on Goal' THEN try_cast(stats_value AS INT) END) AS shots_on_goal,
    MAX(CASE WHEN stats_type = 'Shots off Goal' THEN try_cast(stats_value AS INT) END) AS shots_off_goal,
    MAX(CASE WHEN stats_type = 'Blocked Shots' THEN try_cast(stats_value AS INT) END) AS blocked_shots,
    MAX(CASE WHEN stats_type = 'Total Shots' THEN try_cast(stats_value AS INT) END) AS total_shots,
    MAX(CASE WHEN stats_type = 'Shots insidebox' THEN try_cast(stats_value AS INT) END) AS shots_insidebox,
    MAX(CASE WHEN stats_type = 'Shots outsidebox' THEN try_cast(stats_value AS INT) END) AS shots_outsidebox,
    MAX(CASE WHEN stats_type = 'Fouls' THEN try_cast(stats_value AS INT) END) AS fouls,
    MAX(CASE WHEN stats_type = 'Yellow Cards' THEN coalesce(try_cast(stats_value AS INT), 0) END) AS yellow_cards,
    MAX(CASE WHEN stats_type = 'Red Cards' THEN coalesce(try_cast(stats_value AS INT), 0) END) AS red_cards,
    MAX(CASE WHEN stats_type = 'Corner Kicks' THEN try_cast(stats_value AS INT) END) AS corner_kicks,
    MAX(CASE WHEN stats_type = 'Offsides' THEN coalesce(try_cast(stats_value AS INT), 0) END) AS offsides,
    MAX(CASE WHEN stats_type = 'Goalkeeper Saves' THEN coalesce(try_cast(stats_value AS INT), 0) END) AS goalkeeper_saves,
    MAX(CASE WHEN stats_type = 'Ball Possession' THEN try_cast(replace(stats_value, '%', '') AS DOUBLE)/100 END) AS ball_possession,
    MAX(CASE WHEN stats_type = 'Total passes' THEN try_cast(stats_value AS INT) END) AS total_passes,
    MAX(CASE WHEN stats_type = 'Passes accurate' THEN try_cast(stats_value AS INT) END) AS passes_accurate,
    MAX(CASE WHEN stats_type = 'Passes %' THEN try_cast(replace(stats_value, '%', '') AS DOUBLE)/100 END) AS passes_percentage
from long_format
group by fixture_id, team_id