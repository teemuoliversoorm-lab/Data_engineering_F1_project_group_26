SELECT
    e.Season,
    d.DriverId,
    SUM(r.PlacesGainedLost) AS NetPositions
FROM gold.FactDriverEventResult r
JOIN gold.DimDriver d
    ON r.DriverKey = d.DriverKey
JOIN gold.DimEvent e
    ON r.EventKey = e.EventKey
GROUP BY e.Season, d.DriverId
ORDER BY e.Season, NetPositions DESC NULLS LAST, d.DriverId;
