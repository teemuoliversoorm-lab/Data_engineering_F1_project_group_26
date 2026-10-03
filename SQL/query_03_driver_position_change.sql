SELECT
    d.DriverName,
    SUM(r.PlacesGainedLost) AS net_positions
FROM FactDriverEventResult r
JOIN DimDriver d
    ON r.DriverKey = d.DriverKey
GROUP BY d.DriverName
ORDER BY net_positions DESC;