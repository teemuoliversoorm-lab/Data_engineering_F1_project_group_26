{{ config(indexes=[
    {'columns': ['eventkey', 'driverkey', 'sessiontype', 'lapnumber'], 'unique': true},
    {'columns': ['driverkey']}, {'columns': ['teamkey']},
    {'columns': ['sessiontype', 'lapnumber', 'lapposition']}
]) }}
select
    e.eventkey, d.driverkey, t.teamkey,
    case l.session_type when 'Q' then 'Qualifying' when 'R' then 'Race' end as sessiontype,
    l.lap_number as lapnumber,
    l.lap_position as lapposition,
    l.lap_position_method as lappositionmethod,
    l.lap_position_observed_at as lappositionobservedat,
    l.track_status as trackstatus,
    l.lap_time as laptime
from {{ ref('laps') }} l
join {{ ref('dimevent') }} e on e.season = l.season and e.roundnumber = l.round_number
join {{ ref('dimdriver') }} d on d.driverid = l.driver_id
join {{ ref('dimteam') }} t on t.teamid = l.team_id
