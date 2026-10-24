select
    l.season, l.round_number, l.session_type, l.driver_number,
    r.driver_id, r.team_id, l.lap_number,
    (l.source_record ->> 'Position')::numeric::integer as lap_position,
    l.source_record ->> 'TrackStatus' as track_status,
    l.source_record ->> 'PositionMethod' as lap_position_method,
    (l.source_record ->> 'PositionObservedAt')::timestamptz as lap_position_observed_at,
    extract(epoch from (l.source_record ->> 'LapTime')::interval)::numeric(12,3) as lap_time
from {{ source('bronze', 'laps') }} l
join {{ ref('session_results') }} r
    using (season, round_number, session_type, driver_number)
