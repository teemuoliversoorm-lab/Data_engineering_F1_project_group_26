INSERT INTO DimDriver (
    DriverKey,
    DriverId,
    FullName,
    Abbreviation,
    CountryCode
)
VALUES
(1, 'max_verstappen', 'Max Verstappen', 'VER', 'NED'),
(2, 'norris', 'Lando Norris', 'NOR', 'GBR');


INSERT INTO DimTeam (
    TeamKey,
    TeamId,
    TeamName
)
VALUES
(1, 'red_bull', 'Red Bull Racing'),
(2, 'mclaren', 'McLaren');


INSERT INTO DimEvent (
    EventKey,
    Season,
    RoundNumber,
    EventName,
    Country,
    CircuitName,
    StartDate,
    EndDate
)
VALUES
(
    1,
    2025,
    1,
    'Australian Grand Prix',
    'Australia',
    'Albert Park Grand Prix Circuit',
    '2025-03-14',
    '2025-03-16'
);


INSERT INTO FactDriverEventResult (
    EventKey,
    DriverKey,
    TeamKey,
    QualifyingPosition,
    QualifyingTime,
    GridPosition,
    FinishPosition,
    PlacesGainedLost,
    QualifyingGapToPole,
    Points
)
VALUES
(1, 1, 1, 3, 75.481, 3, 2, 1, 0.385, 18),
(1, 2, 2, 1, 75.096, 1, 1, 0, 0.000, 25);
