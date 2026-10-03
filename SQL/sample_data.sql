INSERT INTO DimDriver
VALUES
(1, 'Max Verstappen', 'Netherlands'),
(2, 'Lando Norris', 'United Kingdom');
 
INSERT INTO DimTeam
VALUES
(1, 'Red Bull Racing'),
(2, 'McLaren');
 
INSERT INTO DimEvent
VALUES
(1, 'Australian Grand Prix', 2025, 'Albert Park');
 
INSERT INTO FactDriverEventResult
VALUES
(1, 1, 1, 1, 1, 0),
(2, 2, 1, 3, 2, 1);