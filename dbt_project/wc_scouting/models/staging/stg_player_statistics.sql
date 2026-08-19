{{ config(materialized='view') }}

with raw_data as (
    select
        fixture_id,
        raw_payload,
        ingested_at
    from {{ source('bronze', 'raw_api_responses') }}
    where endpoint = 'players'
    qualify row_number() over (partition by fixture_id order by ingested_at desc) = 1
),
raw_json as (
    select
        fixture_id,
        from_json(
            raw_payload,
            'response ARRAY<STRUCT<
                team: STRUCT<id: INT, name: STRING>,
                players: ARRAY<STRUCT<
                    player: STRUCT<id: INT, name: STRING>,
                    statistics: ARRAY<STRUCT<
                        games: STRUCT<
                            minutes: STRING,
                            position: STRING,
                            rating: STRING,
                            captain: STRING,
                            substitute: STRING
                        >,
                        offsides: STRING,
                        shots: STRUCT<
                            total: STRING,
                            on: STRING
                        >,
                        goals: STRUCT<
                            total: STRING,
                            conceded: STRING,
                            assists: STRING,
                            saves: STRING
                        >,
                        passes: STRUCT<
                            total: STRING,
                            key: STRING,
                            accuracy: STRING
                        >,
                        tackles: STRUCT<
                            total: STRING,
                            blocks: STRING,
                            interceptions: STRING
                        >,
                        duels: STRUCT<
                            total: STRING,
                            won: STRING
                        >,
                        dribbles: STRUCT<
                            attempts: STRING,
                            success: STRING,
                            past: STRING
                        >,
                        fouls: STRUCT<
                            drawn: STRING,
                            committed: STRING
                        >,
                        cards: STRUCT<
                            yellow: STRING,
                            red: STRING
                        >,
                        penalty: STRUCT<
                            won: STRING,
                            commited: STRING,
                            scored: STRING,
                            missed: STRING,
                            saved: STRING
                        >
                    >>
                >>
            >>'
        ) as j
    from raw_data
),
explode_response as (
    select
        fixture_id,
        explode(j.response) as json_respose
    from raw_json
),
explode_players as (
    select
        fixture_id,
        json_respose.team.id as team_id,
        json_respose.team.name as team_name,
        explode(json_respose.players) as json_player
    from explode_response
),
explode_statistics as (
    select
        fixture_id,
        team_id,
        team_name,
        json_player.player.id as player_id,
        json_player.player.name as player_name,
        explode(json_player.statistics) as json_stats
    from explode_players
)
select
    fixture_id,
    team_id,
    team_name,
    player_id,
    player_name,
    json_stats.games.minutes as minutes,
    json_stats.games.position as position,
    json_stats.games.rating as rating,
    json_stats.games.captain as captain,
    json_stats.games.substitute as substitute,
    json_stats.offsides as offsides,
    json_stats.shots.total as total_shots,
    json_stats.shots.on as shots_on_target,
    json_stats.goals.total as goals_total,
    json_stats.goals.conceded as goals_conceded,
    json_stats.goals.assists as assists,
    json_stats.goals.saves as saves,
    json_stats.passes.total as passes_total,
    json_stats.passes.key as passes_key,
    json_stats.passes.accuracy as passes_accuracy,
    json_stats.tackles.total as tackles_total,
    json_stats.tackles.blocks as tackles_blocks,
    json_stats.tackles.interceptions as tackles_interceptions,
    json_stats.duels.total as duels_total,
    json_stats.duels.won as duels_won,
    json_stats.dribbles.attempts as dribbles_attempts,
    json_stats.dribbles.success as dribbles_success,
    json_stats.dribbles.past as dribbles_past,
    json_stats.fouls.drawn as fouls_drawn,
    json_stats.fouls.committed as fouls_committed,
    json_stats.cards.yellow as cards_yellow,
    json_stats.cards.red as cards_red,
    json_stats.penalty.won as penalty_won,
    json_stats.penalty.commited as penalty_commited,
    json_stats.penalty.scored as penalty_scored,
    json_stats.penalty.missed as penalty_missed,
    json_stats.penalty.saved as penalty_saved
    from explode_statistics