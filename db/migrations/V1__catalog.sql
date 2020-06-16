-- A schema containing metadata for use by telemetry bot
CREATE SCHEMA telemetry_catalog;

-- A list of data series scraped from prometheus '/metrics' endpoints
CREATE TABLE telemetry_catalog.tables (
    id serial PRIMARY KEY,
    name text NOT NULL,
    table_name text NOT NULL,
    series_type text NOT NULL CHECK (series_type IN ('Counter', 'CounterInteger', 'Gauge', 'GaugeInteger')),
    label_columns text[] NOT NULL,
    UNIQUE(name),
    UNIQUE(table_name)
);

CREATE SCHEMA telemetry_data;
CREATE SCHEMA telemetry_series;
CREATE SCHEMA telemetry_metric;
