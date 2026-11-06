{{ config(indexes=[{'columns': ['teamkey'], 'unique': true}]) }}
select distinct on (team_id)
    'team:' || team_id as teamkey,
    team_id as teamid,
    coalesce(team_name, team_id) as teamname
from {{ ref('session_results') }}
order by team_id, season desc, round_number desc, session_type desc
