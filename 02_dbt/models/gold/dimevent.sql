{{ config(indexes=[{'columns': ['eventkey'], 'unique': true}, {'columns': ['season', 'roundnumber'], 'unique': true}]) }}
select
    'event:' || season || ':' || round_number as eventkey,
    season,
    round_number as roundnumber,
    max(event_name) as eventname,
    max(country) as country,
    max(circuit_name) as circuitname,
    min(session_date) as startdate,
    max(event_date) as enddate
from {{ ref('session_results') }}
group by season, round_number
