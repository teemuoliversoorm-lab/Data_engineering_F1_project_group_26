CREATE TABLE DimDriver (
DriverKey INT PRIMARY KEY,
DriverName VARCHAR(100),
Nationality VARCHAR(50)
);
 
CREATE TABLE DimTeam (
TeamKey INT PRIMARY KEY,
TeamName VARCHAR(100)
);
 
CREATE TABLE DimEvent (
EventKey INT PRIMARY KEY,
GrandPrixName VARCHAR(100),
Season INT,
CircuitName VARCHAR(100)
);
 
CREATE TABLE FactDriverEventResult (
DriverKey INT,
TeamKey INT,
EventKey INT,
QualifyingPosition INT,
FinishPosition INT,
PlacesGainedLost INT,
FOREIGN KEY (DriverKey) REFERENCES DimDriver(DriverKey),
FOREIGN KEY (TeamKey) REFERENCES DimTeam(TeamKey),
FOREIGN KEY (EventKey) REFERENCES DimEvent(EventKey)
);