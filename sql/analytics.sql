-- 1. Daily revenue and transaction volume
SELECT
  event_date,
  COUNT(*) AS transaction_count,
  ROUND(SUM(amount_usd), 2) AS revenue_usd
FROM `YOUR_GCP_PROJECT.analytics.curated_transactions`
GROUP BY event_date
ORDER BY event_date;

-- 2. Revenue by customer segment
SELECT
  customer_segment,
  COUNT(*) AS transaction_count,
  ROUND(SUM(amount_usd), 2) AS revenue_usd,
  ROUND(AVG(amount_usd), 2) AS average_transaction_usd
FROM `YOUR_GCP_PROJECT.analytics.curated_transactions`
GROUP BY customer_segment
ORDER BY revenue_usd DESC;

-- 3. Category and country analysis
SELECT
  country,
  category,
  COUNT(*) AS transaction_count,
  ROUND(SUM(amount_usd), 2) AS revenue_usd
FROM `YOUR_GCP_PROJECT.analytics.curated_transactions`
GROUP BY country, category
ORDER BY revenue_usd DESC;

-- 4. Basic data quality monitoring
SELECT
  event_date,
  COUNTIF(transaction_id IS NULL) AS missing_transaction_ids,
  COUNTIF(amount_usd < 0) AS negative_usd_amounts,
  COUNT(*) AS total_rows
FROM `YOUR_GCP_PROJECT.analytics.curated_transactions`
GROUP BY event_date
ORDER BY event_date;
