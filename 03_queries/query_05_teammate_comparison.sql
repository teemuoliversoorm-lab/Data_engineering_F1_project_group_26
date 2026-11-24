SELECT
    e.Season,
    e.EventName,
    t.TeamName,

    d1.DriverId AS Driver1,
    d2.DriverId AS Driver2,

    r1.QualifyingPosition AS Driver1QualifyingPosition,
    r2.QualifyingPosition AS Driver2QualifyingPosition,

    ABS(
        r1.QualifyingPosition - r2.QualifyingPosition
    ) AS QualifyingPositionGap,

    r1.FinishPosition AS Driver1FinishPosition,
    r2.FinishPosition AS Driver2FinishPosition,

    ABS(
        r1.FinishPosition - r2.FinishPosition
    ) AS FinishPositionGap

FROM gold.FactDriverEventResult r1

JOIN gold.FactDriverEventResult r2
    ON r1.EventKey = r2.EventKey
   AND r1.TeamKey = r2.TeamKey
   AND r1.DriverKey < r2.DriverKey

JOIN gold.DimDriver d1
    ON r1.DriverKey = d1.DriverKey

JOIN gold.DimDriver d2
    ON r2.DriverKey = d2.DriverKey

JOIN gold.DimTeam t
    ON r1.TeamKey = t.TeamKey

JOIN gold.DimEvent e
    ON r1.EventKey = e.EventKey

WHERE r1.QualifyingPosition > 0
  AND r2.QualifyingPosition > 0
  AND r1.FinishPosition > 0
  AND r2.FinishPosition > 0

ORDER BY
    e.Season,
    e.EventName,
    t.TeamName;
