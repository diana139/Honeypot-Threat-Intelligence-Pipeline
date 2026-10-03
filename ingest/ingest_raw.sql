-- Rulează din rădăcina proiectului, în baza folosită de dbt și Airflow:
--   duckdb warehouse.duckdb < ingest/ingest_raw.sql

INSTALL httpfs;
LOAD httpfs;

-- Ia credențialele AWS din env vars, ~/.aws/credentials sau rolul IAM (merge și local, și în Airflow)
INSTALL aws;
LOAD aws;
CREATE OR REPLACE SECRET s3_honeypot (
    TYPE s3,
    PROVIDER credential_chain,
    REGION 'eu-north-1'
);

CREATE OR REPLACE TABLE raw_events AS
SELECT *
FROM read_json_auto(
    's3://sundew-honeypot-diana/raw/dt=*/cowrie.json.gz',
    hive_partitioning = true,
    union_by_name = true
);

SELECT count(*) AS rows_loaded FROM raw_events;