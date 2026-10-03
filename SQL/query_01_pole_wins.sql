SELECT
    COUNT(*) AS races,
    SUM(CASE
            WHEN QualifyingPosition = 1
             AND FinishPosition = 1
            THEN 1
            ELSE 0
        END) AS pole_wins,
    ROUND(
        100.0 * SUM(CASE
                        WHEN QualifyingPosition = 1
                         AND FinishPosition = 1
                        THEN 1
                        ELSE 0
                    END)
        / COUNT(*),
        2
    ) AS win_percentage
FROM FactDriverEventResult
WHERE QualifyingPosition = 1;