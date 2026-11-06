{{ config(indexes=[{'columns': ['driverteamassignmentkey'], 'unique': true}, {'columns': ['driverkey', 'effectivedate']}]) }}
with observed as (
    -- Main-race team wins if Q and R disagree within the same event.
    select distinct on (driver_id, season, round_number)
        driver_id, team_id, event_date, season, round_number
    from {{ ref('session_results') }}
    order by driver_id, season, round_number, session_type desc
), previous as (
    select *, lag(team_id) over (partition by driver_id order by season, round_number) as previous_team
    from observed
), changes as (
    select * from previous where previous_team is distinct from team_id
), history as (
    select *, lead(event_date) over (partition by driver_id order by season, round_number) as expiration_date
    from changes
)
select
    'assignment:' || driver_id || ':' || season || ':' || round_number as driverteamassignmentkey,
    'driver:' || driver_id as driverkey,
    'team:' || team_id as teamkey,
    event_date as effectivedate,
    expiration_date as expirationdate,
    expiration_date is null as iscurrent
from history
