SELECT
    t.TeamName,
    SUM(r.PlacesGainedLost) AS net_positions
FROM FactDriverEventResult r
JOIN DimTeam t
    ON r.TeamKey = t.TeamKey
GROUP BY t.TeamName
ORDER BY net_positions DESC;