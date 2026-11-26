WITH teammate_pairs AS (
    SELECT
        e.Season,
        e.EventName,
        r1.TeamKey,
        r1.DriverKey AS Driver1Key,
        r2.DriverKey AS Driver2Key,
        r1.QualifyingPosition AS Driver1QualifyingPosition,
        r2.QualifyingPosition AS Driver2QualifyingPosition,
        r1.FinishPosition AS Driver1FinishPosition,
        r2.FinishPosition AS Driver2FinishPosition,
        CASE
            WHEN r1.QualifyingPosition = r2.QualifyingPosition
              OR r1.FinishPosition = r2.FinishPosition THEN NULL
            ELSE (r1.QualifyingPosition < r2.QualifyingPosition)
               = (r1.FinishPosition < r2.FinishPosition)
        END AS BetterQualifierFinishedAhead
    FROM gold.FactDriverEventResult r1
    JOIN gold.FactDriverEventResult r2
        ON r1.EventKey = r2.EventKey
       AND r1.TeamKey = r2.TeamKey
       AND r1.DriverKey < r2.DriverKey
    JOIN gold.DimEvent e ON r1.EventKey = e.EventKey
    WHERE r1.QualifyingPosition > 0 AND r2.QualifyingPosition > 0
      AND r1.FinishPosition > 0 AND r2.FinishPosition > 0
),
team_season_counts AS (
    SELECT
        p.*,
        COUNT(*) FILTER (WHERE BetterQualifierFinishedAhead)
            OVER team_season AS TeamSeasonAheadPairs,
        COUNT(BetterQualifierFinishedAhead)
            OVER team_season AS TeamSeasonComparablePairs
    FROM teammate_pairs p
    WINDOW team_season AS (PARTITION BY Season, TeamKey)
)
SELECT
    p.Season,
    p.EventName,
    t.TeamName,
    d1.DriverId AS Driver1,
    d2.DriverId AS Driver2,
    p.Driver1QualifyingPosition,
    p.Driver2QualifyingPosition,
    ABS(p.Driver1QualifyingPosition - p.Driver2QualifyingPosition) AS QualifyingPositionGap,
    p.Driver1FinishPosition,
    p.Driver2FinishPosition,
    ABS(p.Driver1FinishPosition - p.Driver2FinishPosition) AS FinishPositionGap,
    p.BetterQualifierFinishedAhead,
    p.TeamSeasonAheadPairs,
    p.TeamSeasonComparablePairs,
    ROUND(100.0 * p.TeamSeasonAheadPairs / NULLIF(p.TeamSeasonComparablePairs, 0), 2)
        AS TeamSeasonAheadPct
FROM team_season_counts p
JOIN gold.DimTeam t ON p.TeamKey = t.TeamKey
JOIN gold.DimDriver d1 ON p.Driver1Key = d1.DriverKey
JOIN gold.DimDriver d2 ON p.Driver2Key = d2.DriverKey
ORDER BY p.Season, p.EventName, t.TeamName, d1.DriverId, d2.DriverId;
