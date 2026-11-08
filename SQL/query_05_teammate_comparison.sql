WITH teammate_pairs AS (
    SELECT 
        r1.event_key,
        r1.team_key,
        r1.driver_key AS driver1_key,
        r2.driver_key AS driver2_key,
        r1.qualifying_position AS d1_qual_pos,
        r2.qualifying_position AS d2_qual_pos,
        (r2.qualifying_position - r1.qualifying_position) AS qual_position_gap,
        r1.finishing_position AS d1_race_pos,
        r2.finishing_position AS d2_race_pos,
        (r2.finishing_position - r1.finishing_position) AS race_position_gap
    FROM gold.factdrivereventresult r1
    JOIN gold.factdrivereventresult r2 
      ON r1.event_key = r2.event_key 
     AND r1.team_key = r2.team_key 
     AND r1.driver_key < r2.driver_key 
)
SELECT 
    e.season,
    e.official_event_name,
    t.team_name,
    d1.driver_name AS driver_1,
    d2.driver_name AS driver_2,
    tp.d1_qual_pos,
    tp.d2_qual_pos,
    ABS(tp.qual_position_gap) AS qual_gap_positions,
    tp.d1_race_pos,
    tp.d2_race_pos,
    ABS(tp.race_position_gap) AS race_gap_positions
FROM teammate_pairs tp
JOIN gold.dimevent e ON tp.event_key = e.event_key
JOIN gold.dimteam t ON tp.team_key = t.team_key
JOIN gold.dimdriver d1 ON tp.driver1_key = d1.driver_key
JOIN gold.dimdriver d2 ON tp.driver2_key = d2.driver_key
ORDER BY e.season ASC, e.event_key, t.team_name;
