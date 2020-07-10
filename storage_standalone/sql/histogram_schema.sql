CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA_HISTOGRAM;

CREATE TABLE IF NOT EXISTS SCHEMA_CATALOG.histogram (
    id int4 PRIMARY KEY,
    metric_name text NOT NULL,
    table_name name NOT NULL,
    creation_completed boolean NOT NULL DEFAULT FALSE
);

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.create_histogram_table(
    histogram_name text,
    OUT id integer,
    OUT table_name name
)
 RETURNS record
 LANGUAGE plpgsql
AS $function$
BEGIN
LOOP
    INSERT INTO SCHEMA_CATALOG.histogram (id, metric_name, table_name)
        SELECT  id,
                metric_name,
                table_name
        FROM SCHEMA_CATALOG.metric
        WHERE metric_name = histogram_name
    ON CONFLICT DO NOTHING
    RETURNING SCHEMA_CATALOG.histogram.id,
              SCHEMA_CATALOG.histogram.table_name
    INTO id, table_name;
    EXIT WHEN FOUND;

    SELECT h.id, h.table_name
    INTO id, table_name
    FROM SCHEMA_CATALOG.histogram h
    WHERE metric_name = histogram_name;

    EXIT WHEN FOUND;
END LOOP;
END
$function$;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_or_create_histogram_table_name(
        histogram_name text, OUT id int, OUT table_name name, OUT possibly_new BOOLEAN)
AS $func$
   SELECT id, table_name::name, false
   FROM SCHEMA_CATALOG.metric m
   WHERE m.metric_name = histogram_name
   UNION ALL
   SELECT *, true
   FROM SCHEMA_CATALOG.create_histogram_table(histogram_name)
   LIMIT 1
$func$
LANGUAGE SQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_or_create_histogram_table_name(text) to prom_writer;

--This procedure finalizes the creation of a histogram. The first part of
--histogram creation happens in make_histogram_table and the final part happens here.
--We split histogram creation into two parts to minimize latency during insertion
--(which happens in the make_histogram_table path).
--
--lock-order: histogram table, data_table
CREATE OR REPLACE PROCEDURE SCHEMA_CATALOG.finalize_histogram_creation()
AS $proc$
DECLARE
    r RECORD;
    created boolean;
BEGIN
    FOR r IN
        SELECT *
        FROM SCHEMA_CATALOG.histogram
        WHERE NOT creation_completed
        ORDER BY random()
    LOOP
        SELECT creation_completed
        INTO created
        FROM SCHEMA_CATALOG.histogram m
        WHERE m.id = r.id
        FOR UPDATE;

        IF created THEN
            --release row lock
            COMMIT;
            CONTINUE;
        END IF;

        EXECUTE format($$
            ALTER TABLE SCHEMA_DATA_HISTOGRAM.%I SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'series_id',
                timescaledb.compress_orderby = 'time'
            ); $$, r.table_name);

        --chunks where the end time is before now()-1 hour will be compressed
        PERFORM add_compress_chunks_policy(format('SCHEMA_DATA_HISTOGRAM.%I', r.table_name), INTERVAL '1 hour');

        --do this before taking exclusive lock to minimize work after taking lock
        UPDATE SCHEMA_CATALOG.histogram SET creation_completed = TRUE WHERE id = r.id;
    END LOOP;
END;
$proc$ LANGUAGE PLPGSQL;
COMMENT ON PROCEDURE SCHEMA_CATALOG.finalize_histogram_creation()
IS 'Finalizes histogram creation. This procedure should be run by telemetry bot automatically';
GRANT EXECUTE ON PROCEDURE SCHEMA_CATALOG.finalize_histogram_creation() TO prom_writer;

--This function is called by a trigger when a new histogram is created. It
--sets up the histogram just enough to insert data into it. histogram creation
--is completed in finalize_histogram_creation() above. See the comments
--on that function for the reasoning for this split design.
--
--Note: latency-sensitive function. Should only contain just enough logic
--to support inserts for the histogram.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.make_histogram_table()
    RETURNS trigger
    AS $func$
BEGIN
    EXECUTE format('CREATE TABLE SCHEMA_DATA_HISTOGRAM.%I(time TIMESTAMPTZ NOT NULL, series_id INT NOT NULL, count DOUBLE PRECISION, sum DOUBLE PRECISION, le jsonb NOT NULL)',
                    NEW.table_name);
    EXECUTE format('CREATE INDEX data_histogram_series_id_time_%s ON SCHEMA_DATA_HISTOGRAM.%I (series_id, time) INCLUDE (count, sum)',
                    NEW.id, NEW.table_name);
    PERFORM create_hypertable(format('SCHEMA_DATA_HISTOGRAM.%I', NEW.table_name), 'time',
                             chunk_time_interval=>SCHEMA_CATALOG.get_default_chunk_interval(),
                             create_default_indexes=>false);

   RETURN NEW;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.make_histogram_table() TO prom_writer;

CREATE OR REPLACE TRIGGER make_histogram_table_trigger
    AFTER INSERT ON SCHEMA_CATALOG.histogram
    FOR EACH ROW
    EXECUTE PROCEDURE SCHEMA_CATALOG.make_histogram_table();
