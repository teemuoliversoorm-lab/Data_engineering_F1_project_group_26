SELECT
    e.Season,
    e.EventName,
    e.CircuitName,
    d.DriverId,
    COUNT(*) OVER (
        PARTITION BY e.CircuitName
    ) AS RetainedLeadCountAtCircuit
FROM gold.FactLap l
JOIN gold.DimEvent e
    ON l.EventKey = e.EventKey
JOIN gold.DimDriver d
    ON l.DriverKey = d.DriverKey
JOIN gold.FactDriverEventResult r
    ON l.EventKey = r.EventKey
   AND l.DriverKey = r.DriverKey
WHERE r.GridPosition = 1
  AND l.LapNumber = 1
  AND l.LapPosition = 1s
  AND l.SessionType = 'Race'
ORDER BY
    e.Season,
    e.EventName;
