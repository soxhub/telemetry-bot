-- Unique metrics
CREATE TABLE public.prometheus_metrics (
    id serial PRIMARY KEY,
    name text NOT NULL,
    type text NOT NULL,
    last_scrape timestamp,
    UNIQUE(name)
);

-- Create a schema to store metric timeseries data
CREATE SCHEMA prometheus_series;
