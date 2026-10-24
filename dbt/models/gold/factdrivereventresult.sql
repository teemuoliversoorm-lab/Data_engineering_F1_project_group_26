{{ config(indexes=[
    {'columns': ['eventkey', 'driverkey'], 'unique': true},
    {'columns': ['driverkey']}, {'columns': ['teamkey']}
]) }}
with qualifying as (
    select *,
        coalesce(q3_seconds, q2_seconds, q1_seconds) as qualifying_time,
        case when q3_seconds is not null then 'Q3'
             when q2_seconds is not null then 'Q2'
             when q1_seconds is not null then 'Q1' end as qualifying_stage
    from {{ ref('session_results') }} where session_type = 'Q'
), pole as (
    select season, round_number, qualifying_time, qualifying_stage
    from qualifying where position = 1
)
select
    e.eventkey, d.driverkey, t.teamkey,
    q.position as qualifyingposition,
    q.qualifying_time as qualifyingtime,
    r.grid_position as gridposition,
    r.position as finishposition,
    r.classification_available as classificationavailable,
    q.classification_available as qualifyingclassificationavailable,
    case when r.grid_position > 0 and r.position > 0
         then r.grid_position - r.position end as placesgainedlost,
    q.qualifying_time - p.qualifying_time as qualifyinggaptopole,
    r.points,
    r.status as resultstatus,
    q.qualifying_stage as qualifyingstage,
    p.qualifying_stage as polestage,
    q.qualifying_stage = p.qualifying_stage as samestagecomparison
from {{ ref('session_results') }} r
left join qualifying q using (season, round_number, driver_id)
left join pole p using (season, round_number)
join {{ ref('dimevent') }} e on e.season = r.season and e.roundnumber = r.round_number
join {{ ref('dimdriver') }} d on d.driverid = r.driver_id
join {{ ref('dimteam') }} t on t.teamid = r.team_id
where r.session_type = 'R'
