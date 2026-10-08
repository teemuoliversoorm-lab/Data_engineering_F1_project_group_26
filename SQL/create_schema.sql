CREATE TABLE DimDriver (
    DriverKey INT PRIMARY KEY,
    DriverId VARCHAR(50) NOT NULL UNIQUE,
    DriverNumber INT,
    FullName VARCHAR(100) NOT NULL,
    Abbreviation CHAR(3),
    CountryCode CHAR(3)
);


CREATE TABLE DimTeam (
    TeamKey INT PRIMARY KEY,
    TeamId VARCHAR(50) NOT NULL UNIQUE,
    TeamName VARCHAR(100) NOT NULL
);


CREATE TABLE DimEvent (
    EventKey INT PRIMARY KEY,
    Season INT NOT NULL,
    RoundNumber INT NOT NULL,
    EventName VARCHAR(100) NOT NULL,
    Country VARCHAR(100),
    CircuitName VARCHAR(100),
    StartDate DATE,
    EndDate DATE
);


CREATE TABLE DimDriverTeamAssignment (
    DriverTeamAssignmentKey INT PRIMARY KEY,
    DriverKey INT NOT NULL,
    TeamKey INT NOT NULL,
    EffectiveDate DATE NOT NULL,
    ExpirationDate DATE,
    IsCurrent BOOLEAN NOT NULL,

    CONSTRAINT fk_assignment_driver
        FOREIGN KEY (DriverKey)
        REFERENCES DimDriver(DriverKey),

    CONSTRAINT fk_assignment_team
        FOREIGN KEY (TeamKey)
        REFERENCES DimTeam(TeamKey)
);

CREATE TABLE FactLap (
    EventKey INT NOT NULL,
    DriverKey INT NOT NULL,
    TeamKey INT NOT NULL,
    SessionType VARCHAR(25) NOT NULL,
    LapNumber INT NOT NULL,
    LapPosition INT,
    TrackStatus VARCHAR(20),
    LapTime DECIMAL(8,3),

    PRIMARY KEY (
        EventKey,
        DriverKey,
        SessionType,
        LapNumber
    ),

    CONSTRAINT fk_lap_event
        FOREIGN KEY (EventKey)
        REFERENCES DimEvent(EventKey),

    CONSTRAINT fk_lap_driver
        FOREIGN KEY (DriverKey)
        REFERENCES DimDriver(DriverKey),

    CONSTRAINT fk_lap_team
        FOREIGN KEY (TeamKey)
        REFERENCES DimTeam(TeamKey)
);


CREATE TABLE FactDriverEventResult (
    EventKey INT NOT NULL,
    DriverKey INT NOT NULL,
    TeamKey INT NOT NULL,
    QualifyingPosition INT,
    QualifyingTime DECIMAL(8,3),
    GridPosition INT,
    FinishPosition INT,
    PlacesGainedLost INT,
    QualifyingGapToPole DECIMAL(8,3),
    Points INT,

    PRIMARY KEY (EventKey, DriverKey),

    CONSTRAINT fk_result_event
        FOREIGN KEY (EventKey)
        REFERENCES DimEvent(EventKey),

    CONSTRAINT fk_result_driver
        FOREIGN KEY (DriverKey)
        REFERENCES DimDriver(DriverKey),

    CONSTRAINT fk_result_team
        FOREIGN KEY (TeamKey)
        REFERENCES DimTeam(TeamKey)
);
