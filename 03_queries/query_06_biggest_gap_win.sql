-- Winners with an unknown qualifying gap (e.g. a session OpenF1 never
-- published, such as Baku 2025 Qualifying) must be excluded explicitly:
-- Postgres sorts NULL as the "largest" value in DESC order by default, which
-- would otherwise pick an unknown gap over every real one.
SELECT
    e.EventName,
    e.Season,
    d.DriverId,
    r.QualifyingPosition,
    r.QualifyingGapToPole
FROM FactDriverEventResult r
JOIN DimDriver d
    ON r.DriverKey = d.DriverKey
JOIN DimEvent e
    ON r.EventKey = e.EventKey
WHERE r.FinishPosition = 1
    AND r.QualifyingGapToPole IS NOT NULL
ORDER BY r.QualifyingGapToPole DESC
LIMIT 1;
