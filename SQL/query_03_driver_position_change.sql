SELECT 
    e.season,
    d.driver_name,
    t.team_name AS primary_team,
    COUNT(*) FILTER (WHERE r.position_change_delta IS NOT NULL) AS classified_starts,
    SUM(r.position_change_delta) AS net_positions_gained_lost,
    ROUND(AVG(r.position_change_delta), 2) AS avg_positions_gained_per_race
FROM gold.factdrivereventresult r
JOIN gold.dimevent e ON r.event_key = e.event_key
JOIN gold.dimdriver d ON r.driver_key = d.driver_key
JOIN gold.dimteam t ON r.team_key = t.team_key
WHERE r.position_change_delta IS NOT NULL
GROUP BY e.season, d.driver_name, t.team_name
ORDER BY e.season ASC, net_positions_gained_lost DESC;
