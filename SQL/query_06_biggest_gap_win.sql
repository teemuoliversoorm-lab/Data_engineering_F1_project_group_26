WITH pole_times AS (
    SELECT 
        r.event_key,
        r.best_qualifying_time_seconds AS pole_time_seconds
    FROM gold.factdrivereventresult r
    WHERE r.starting_grid_position = 1
)
SELECT 
    e.season,
    e.official_event_name,
    d.driver_name,
    t.team_name,
    r.starting_grid_position AS winner_grid_pos,
    r.best_qualifying_time_seconds AS winner_qual_time,
    pt.pole_time_seconds,
    ROUND(CAST(r.best_qualifying_time_seconds - pt.pole_time_seconds AS numeric), 3) AS qual_time_gap_seconds
FROM gold.factdrivereventresult r
JOIN gold.dimevent e ON r.event_key = e.event_key
JOIN gold.dimdriver d ON r.driver_key = d.driver_key
JOIN gold.dimteam t ON r.team_key = t.team_key
JOIN pole_times pt ON r.event_key = pt.event_key
WHERE r.finishing_position = 1 
ORDER BY qual_time_gap_seconds DESC NULLS LAST;
