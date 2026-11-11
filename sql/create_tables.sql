-- BigQuery Standard SQL
CREATE SCHEMA IF NOT EXISTS `YOUR_GCP_PROJECT.analytics`;

CREATE TABLE IF NOT EXISTS `YOUR_GCP_PROJECT.analytics.curated_transactions` (
  transaction_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  transaction_ts TIMESTAMP NOT NULL,
  amount NUMERIC NOT NULL,
  currency STRING NOT NULL,
  country STRING,
  category STRING,
  customer_segment STRING,
  amount_usd NUMERIC,
  event_date DATE NOT NULL,
  ingested_at TIMESTAMP
)
PARTITION BY event_date
CLUSTER BY country, category, customer_segment;

CREATE TABLE IF NOT EXISTS `YOUR_GCP_PROJECT.analytics.rejected_transactions` (
  raw_record JSON,
  rejection_reason STRING,
  rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(rejected_at);
