{{ config(materialized='table') }}

with match_results as (
    select
        fixture_id,
        match_date,
        tournament_name,
        tournament_round,
        home_team_id as team_id,
        home_team_name as team_name,
        home_goals as goals_for,
        away_goals as goals_against,
        case
            when home_goals > away_goals then 'W'
            when home_goals < away_goals then 'L'
            else 'D'
        end as result
    from {{ ref('stg_fixtures') }}

    union all

    select
        fixture_id,
        match_date,
        tournament_name,
        tournament_round,
        away_team_id as team_id,
        away_team_name as team_name,
        away_goals as goals_for,
        home_goals as goals_against,
        case
            when away_goals > home_goals then 'W'
            when away_goals < home_goals then 'L'
            else 'D'
        end as result
    from {{ ref('stg_fixtures') }}
),
stats_by_team as (
    select
        fixture_id,
        team_id,
        ball_possession,
        passes_percentage
    from {{ ref('int_team_match_stats') }}
)
select
    mr.team_id,
    mr.tournament_name,
    max(mr.team_name) as team_name,
    count(mr.fixture_id) as matches_played,
    sum(case
        when mr.result = 'W' then 3
        when mr.result = 'D' then 1
        else 0
    end) as points,
    sum(case when mr.result = 'W' then 1 else 0 end) as total_wins,
    sum(case when mr.result = 'D' then 1 else 0 end) as total_draws,
    sum(case when mr.result = 'L' then 1 else 0 end) as total_losses,
    sum(mr.goals_for) as total_goals,
    round(avg(mr.goals_for), 2) as avg_goals_for,
    sum(mr.goals_against) as total_goals_against,
    round(avg(mr.goals_against), 2) as avg_goals_against,
    round(avg(s.ball_possession), 2) as avg_ball_possession,
    round(avg(s.passes_percentage), 2) as avg_passes_percentage,
    round(sum(case
        when mr.result = 'W' then 3
        when mr.result = 'D' then 1
        else 0
    end) / 21 * 100, 2) as points_pct
from match_results mr
join stats_by_team s on mr.fixture_id = s.fixture_id and mr.team_id = s.team_id
group by mr.tournament_name, mr.team_id
order by mr.tournament_name, points_pct desc, total_wins desc, total_goals desc, avg_ball_possession desc
