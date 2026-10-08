SELECT
    e.EventName,
    d.DriverId
FROM FactLap l
JOIN DimEvent e
    ON l.EventKey = e.EventKey
JOIN DimDriver d
    ON l.DriverKey = d.DriverKey
JOIN FactDriverEventResult r
    ON l.EventKey = r.EventKey
   AND l.DriverKey = r.DriverKey
WHERE r.QualifyingPosition = 1
  AND l.LapNumber = 1
  AND l.LapPosition = 1
  AND l.SessionType = 'Race';
