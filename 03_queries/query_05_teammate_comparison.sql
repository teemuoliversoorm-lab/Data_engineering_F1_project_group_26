SELECT
    e.EventName,
    t.TeamName,
    d.DriverId,
    r.QualifyingPosition,
    r.FinishPosition,
    r.QualifyingGapToPole
FROM FactDriverEventResult r
JOIN DimDriver d
    ON r.DriverKey = d.DriverKey
JOIN DimTeam t
    ON r.TeamKey = t.TeamKey
JOIN DimEvent e
    ON r.EventKey = e.EventKey
ORDER BY e.EventName, t.TeamName;
