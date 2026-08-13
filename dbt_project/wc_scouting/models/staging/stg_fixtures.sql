with bronze_fixtures as (
    select raw_payload
    from {{ source('bronze', 'raw_api_responses') }}
    where
        endpoint = 'fixtures'
        AND league_id IS NOT NULL
        AND season IS NOT NULL
    qualify row_number() over (partition by league_id, season order by ingested_at desc) = 1
),
parsed as (
    select
        from_json(
            raw_payload,
            'response ARRAY<STRUCT<
                fixture: STRUCT<id: BIGINT, date: STRING, referee: STRING,
                    status: STRUCT<short: STRING, long: STRING>,
                    venue: STRUCT<name: STRING, city: STRING>>,
                league: STRUCT<name: STRING, season: INT, round: STRING>,
                teams: STRUCT<
                    home: STRUCT<id: BIGINT, name: STRING, code: STRING>,
                    away: STRUCT<id: BIGINT, name: STRING, code: STRING>>,
                goals: STRUCT<home: INT, away: INT>
            >>',
            map('mode', 'FAILFAST')
        ) as j
    from bronze_fixtures
),
exploded as (
    select
        explode(j.response) as r
    from parsed
)
select
    r.fixture.id as fixture_id,
    r.fixture.date as match_date,
    r.fixture.referee as referee,
    count(*) over(partition by r.fixture.referee) as referee_games,
    r.fixture.status.long as match_status,
    r.fixture.venue.name as venue_name,
    r.fixture.venue.city as venue_city,
    r.league.name as tournament_name,
    r.league.season as tournament_season,
    r.league.round as tournament_round,
    r.teams.home.id as home_team_id,
    r.teams.home.name as home_team_name,
    r.teams.away.id as away_team_id,
    r.teams.away.name as away_team_name,
    r.goals.home as home_goals,
    r.goals.away as away_goals
from exploded
where r.fixture.id is not null