-- Only winners with a measured qualifying gap can answer this question.
SELECT
    e.EventName,
    e.Season,
    d.DriverId,
    r.QualifyingPosition,
    r.QualifyingGapToPole
FROM gold.FactDriverEventResult r
JOIN gold.DimDriver d
    ON r.DriverKey = d.DriverKey
JOIN gold.DimEvent e
    ON r.EventKey = e.EventKey
WHERE r.FinishPosition = 1
    AND r.QualifyingGapToPole IS NOT NULL
ORDER BY r.QualifyingGapToPole DESC
LIMIT 1;
