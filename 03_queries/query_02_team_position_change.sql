SELECT
    e.Season,
    t.TeamName,
    SUM(r.PlacesGainedLost) AS NetPositions
FROM gold.FactDriverEventResult r
JOIN gold.DimTeam t
    ON r.TeamKey = t.TeamKey
JOIN gold.DimEvent e
    ON r.EventKey = e.EventKey
GROUP BY e.Season, t.TeamName
ORDER BY e.Season, NetPositions DESC, t.TeamName;
