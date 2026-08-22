{{ config(materialized='table') }}

with long_format as (
    select *
    from {{ ref('stg_player_statistics') }}
    where minutes is not null
)
select
fixture_id,
    team_id,
    team_name,
    player_id,
    player_name,
    try_cast(minutes AS INT),
    position,
    try_cast(rating AS DOUBLE),
    try_cast(captain AS BOOLEAN),
    try_cast(substitute AS BOOLEAN),
    try_cast(offsides AS INT),
    try_cast(total_shots AS INT),
    try_cast(shots_on_target AS INT),
    try_cast(goals_total AS INT),
    try_cast(goals_conceded AS INT),
    try_cast(assists AS INT),
    try_cast(saves AS INT),
    try_cast(passes_total AS INT),
    try_cast(passes_key AS INT),
    try_cast(passes_accuracy AS INT),
    try_cast(tackles_total AS INT),
    try_cast(tackles_blocks AS INT),
    try_cast(tackles_interceptions AS INT),
    try_cast(duels_total AS INT),
    try_cast(duels_won AS INT),
    try_cast(dribbles_attempts AS INT),
    try_cast(dribbles_success AS INT),
    try_cast(dribbles_past AS INT),
    try_cast(fouls_drawn AS INT),
    try_cast(fouls_committed AS INT),
    try_cast(cards_yellow AS INT),
    try_cast(cards_red AS INT),
    try_cast(penalty_won AS INT),
    try_cast(penalty_commited AS INT),
    try_cast(penalty_scored AS INT),
    try_cast(penalty_missed AS INT),
    try_cast(penalty_saved AS INT)
from long_format
