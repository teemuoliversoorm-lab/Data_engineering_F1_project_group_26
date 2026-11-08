SELECT 
    e.season,
    COUNT(*) AS total_races_with_pole,
    COUNT(*) FILTER (WHERE r.finishing_position = 1) AS pole_wins,
    ROUND(
        COUNT(*) FILTER (WHERE r.finishing_position = 1) * 100.0 / NULLIF(COUNT(*), 0), 
        2
    ) AS pole_win_percentage
FROM gold.factdrivereventresult r
JOIN gold.dimevent e ON r.event_key = e.event_key
WHERE r.starting_grid_position = 1
GROUP BY e.season
ORDER BY e.season;
