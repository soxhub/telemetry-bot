-- The content of this file is licensed under the Timescale Prometheus project (Apache License)
-- and copyright for the contents belongs to the Timescale Prometheus authors.
--
-- Please see https://github.com/timescale/timescale-prometheus/blob/master/LICENSE for details.
--


DO $$
    BEGIN
        CREATE ROLE prom_reader;
    EXCEPTION WHEN duplicate_object THEN
        RAISE NOTICE 'role prom_reader already exists, skipping create';
        RETURN;
    END
$$;
DO $$
    BEGIN
        CREATE ROLE prom_writer;
    EXCEPTION WHEN duplicate_object THEN
        RAISE NOTICE 'role prom_writer already exists, skipping create';
        RETURN;
    END
$$;
GRANT prom_reader TO prom_writer;