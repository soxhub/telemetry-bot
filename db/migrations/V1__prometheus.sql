
-- Unique metrics
CREATE TABLE public.prometheus_metrics (
    id serial PRIMARY KEY,
    name text NOT NULL,
    type text NOT NULL,
    UNIQUE(name)
);

-- Unique label names
CREATE TABLE public.prometheus_label_keys (
    id serial PRIMARY KEY,
    key text NOT NULL,
    UNIQUE(key)
);

-- Unique label values
CREATE TABLE public.prometheus_label_values (
    id serial PRIMARY KEY,
    value text NOT NULL,
    UNIQUE(value)
);

-- Possible labels for for each metric
CREATE TABLE public.prometheus_metric_labels (
    metric_id int NOT NULL REFERENCES public.prometheus_metrics,
    label_id int NOT NULL REFERENCES public.prometheus_label_keys,
    PRIMARY KEY (metric_id, label_id)
);

-- Create a schema to store metric data table
CREATE SCHEMA prometheus_series;

-- CREATE TABLE prometheus_series.labels___<METRIC_NAME>
--     id serial PRIMARY KEY,
--     label_one int,
--     label_two int,
--     label_three int,
--     label_column int,
--     label_column int,
--     additional_tags JSONB,
-- );

-- CREATE TABLE prometheus_series.values___<METRIC_NAME>
--     id bigserial PRIMARY KEY,
--     labels_id int NOT NULL,
--     min_timestamp timestamp NOT NULL,
--     max_timestamp timestamp NOT NULL,
--     timestamps bytea NOT NULL
--     values bytea NOT NULL,
--
--     WITH toast_tuple_target(64)
-- );
-- ALTER TABLE prometheus_series.values___<METRIC_NAME>
--   ALTER COLUMN values SET STORAGE EXTERNAL
--   ALTER COLUMN values SET STORAGE EXTERNAL
-- ;
-- CREATE INDEX values___timestamps_idx ON prometheus_series.values__<METRIC_NAME> (max_timestamp DESC, min_timestamp DESC);
-- CREATE INDEX values___labels_idx ON prometheus_series.values__<METRIC_NAME> (labels_id, max_timestamp DESC, min_timestamp DESC);
