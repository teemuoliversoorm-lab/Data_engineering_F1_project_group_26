WITH pole_sitters AS (
    SELECT 
        r.event_key,
        r.driver_key
    FROM gold.factdrivereventresult r
    WHERE r.starting_grid_position = 1
),
lap1_leaders AS (
    SELECT 
        l.event_key,
        l.driver_key
    FROM gold.factlap l
    WHERE l.session_key LIKE '%_R' 
      AND l.lap_number = 1 
      AND l.lap_position = 1
)
SELECT 
    e.season,
    e.circuit_name,
    COUNT(*) AS total_races,
    COUNT(l.driver_key) AS pole_led_lap_1_count,
    ROUND(
        COUNT(l.driver_key) * 100.0 / NULLIF(COUNT(*), 0), 
        2
    ) AS retention_rate_pct
FROM pole_sitters p
JOIN gold.dimevent e ON p.event_key = e.event_key
LEFT JOIN lap1_leaders l ON p.event_key = l.event_key AND p.driver_key = l.driver_key
GROUP BY e.season, e.circuit_name
ORDER BY e.season ASC, retention_rate_pct DESC;
