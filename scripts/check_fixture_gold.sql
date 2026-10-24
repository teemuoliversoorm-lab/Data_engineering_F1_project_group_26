-- Run only against the isolated database prepared by prepare_fixture_database.py.
DO $$
BEGIN
    IF (SELECT count(*) FROM gold.factlap) <> 8 THEN
        RAISE EXCEPTION 'Expected eight fixture lap facts';
    END IF;
    IF (SELECT count(*) FROM gold.factdrivereventresult) <> 2 THEN
        RAISE EXCEPTION 'Expected two fixture result facts';
    END IF;
    IF (SELECT count(*) FROM gold.dimdriver) <> 2
       OR (SELECT count(*) FROM gold.dimteam) <> 1
       OR (SELECT count(*) FROM gold.dimevent) <> 1
       OR (SELECT count(*) FROM gold.dimdriverteamassignment) <> 2 THEN
        RAISE EXCEPTION 'Unexpected fixture dimensions';
    END IF;
    IF EXISTS (SELECT 1 FROM gold.factlap WHERE laptime <> 90) THEN
        RAISE EXCEPTION 'Duration conversion must give 90 seconds';
    END IF;
    IF EXISTS (SELECT 1 FROM gold.factdrivereventresult
               WHERE qualifyingtime <> 80 OR qualifyingstage <> 'Q3'
                  OR qualifyinggaptopole <> 0 OR placesgainedlost <> 0) THEN
        RAISE EXCEPTION 'Unexpected fixture result metrics';
    END IF;
END $$;
