SELECT
    e.EventName,
    d.DriverId,
    r.QualifyingPosition,
    r.QualifyingGapToPole
FROM FactDriverEventResult r
JOIN DimDriver d
    ON r.DriverKey = d.DriverKey
JOIN DimEvent e
    ON r.EventKey = e.EventKey
WHERE r.FinishPosition = 1
ORDER BY r.QualifyingGapToPole DESC
LIMIT 1;
