-- ops/20_ops_customer_data_quality_daily.sql
-- Purpose:
--   raw -> clean -> mart の品質指標を日次で記録し、異常検知に利用する。
-- Notes:
--   run_date（Asia/Tokyo）単位で1行をUPSERTする（同日再実行は上書き）。

DECLARE run_date DATE DEFAULT CURRENT_DATE("Asia/Tokyo");

MERGE `ec-data-platform.ops.customer_data_quality_daily` T
USING (
  WITH
  raw_base AS (
    SELECT customer_id, email, created_at
    FROM `ec-data-platform.raw.raw_customers`
  ),
  raw_metrics AS (
    SELECT
      COUNT(*) AS raw_row_count,
      COUNTIF(customer_id IS NULL OR TRIM(customer_id) = "") AS raw_customer_id_empty_count,
      COUNTIF(
        created_at IS NULL
        OR TRIM(created_at) = ""
        OR LOWER(TRIM(created_at)) = "created_at"
        OR SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', created_at) IS NULL
      ) AS raw_created_at_invalid_count
    FROM raw_base
  ),
  raw_dupe AS (
    SELECT
      COUNTIF(cnt > 1) AS raw_duplicate_customer_id_keys,
      SUM(IF(cnt > 1, cnt - 1, 0)) AS raw_duplicate_customer_id_rows
    FROM (
      SELECT customer_id, COUNT(*) AS cnt
      FROM raw_base
      WHERE customer_id IS NOT NULL AND TRIM(customer_id) != ""
      GROUP BY customer_id
    )
  ),
  clean_base AS (
    SELECT customer_id, created_at
    FROM `ec-data-platform.clean.customers`
  ),
  clean_metrics AS (
    SELECT
      COUNT(*) AS clean_row_count,
      COUNTIF(customer_id IS NULL OR TRIM(customer_id) = "") AS clean_customer_id_empty_count,
      COUNTIF(created_at IS NULL) AS clean_created_at_null_count
    FROM clean_base
  ),
  mart_metrics AS (
    SELECT
      (SELECT IFNULL(SUM(new_customers), 0)
       FROM `ec-data-platform.mart.customer_daily_signups`
       WHERE signup_date >= DATE_SUB(run_date, INTERVAL 90 DAY)
      ) AS mart_daily_signups_sum_90d,

      (SELECT IFNULL(MAX(cumulative_customers), 0)
       FROM `ec-data-platform.mart.customer_daily_cumulative`
      ) AS mart_latest_cumulative
  )
  SELECT
    run_date,
    raw_metrics.raw_row_count,
    clean_metrics.clean_row_count,
    raw_metrics.raw_created_at_invalid_count,
    clean_metrics.clean_created_at_null_count,
    (raw_metrics.raw_customer_id_empty_count + clean_metrics.clean_customer_id_empty_count) AS customer_id_empty_count,
    raw_dupe.raw_duplicate_customer_id_keys,
    raw_dupe.raw_duplicate_customer_id_rows,
    mart_metrics.mart_daily_signups_sum_90d,
    mart_metrics.mart_latest_cumulative,
    IF(raw_metrics.raw_row_count = 0, TRUE, FALSE) AS alert_raw_zero,
    IF(clean_metrics.clean_row_count = 0, TRUE, FALSE) AS alert_clean_zero,
    IF(raw_metrics.raw_created_at_invalid_count > 0, TRUE, FALSE) AS alert_raw_created_at_invalid,
    IF(clean_metrics.clean_created_at_null_count > 0, TRUE, FALSE) AS alert_clean_created_at_null
  FROM raw_metrics
  CROSS JOIN raw_dupe
  CROSS JOIN clean_metrics
  CROSS JOIN mart_metrics
) S
ON T.run_date = S.run_date
WHEN MATCHED THEN UPDATE SET
  raw_row_count = S.raw_row_count,
  clean_row_count = S.clean_row_count,
  raw_created_at_invalid_count = S.raw_created_at_invalid_count,
  clean_created_at_null_count = S.clean_created_at_null_count,
  customer_id_empty_count = S.customer_id_empty_count,
  raw_duplicate_customer_id_keys = S.raw_duplicate_customer_id_keys,
  raw_duplicate_customer_id_rows = S.raw_duplicate_customer_id_rows,
  mart_daily_signups_sum_90d = S.mart_daily_signups_sum_90d,
  mart_latest_cumulative = S.mart_latest_cumulative,
  alert_raw_zero = S.alert_raw_zero,
  alert_clean_zero = S.alert_clean_zero,
  alert_raw_created_at_invalid = S.alert_raw_created_at_invalid,
  alert_clean_created_at_null = S.alert_clean_created_at_null
WHEN NOT MATCHED THEN INSERT (
  run_date,
  raw_row_count,
  clean_row_count,
  raw_created_at_invalid_count,
  clean_created_at_null_count,
  customer_id_empty_count,
  raw_duplicate_customer_id_keys,
  raw_duplicate_customer_id_rows,
  mart_daily_signups_sum_90d,
  mart_latest_cumulative,
  alert_raw_zero,
  alert_clean_zero,
  alert_raw_created_at_invalid,
  alert_clean_created_at_null
) VALUES (
  S.run_date,
  S.raw_row_count,
  S.clean_row_count,
  S.raw_created_at_invalid_count,
  S.clean_created_at_null_count,
  S.customer_id_empty_count,
  S.raw_duplicate_customer_id_keys,
  S.raw_duplicate_customer_id_rows,
  S.mart_daily_signups_sum_90d,
  S.mart_latest_cumulative,
  S.alert_raw_zero,
  S.alert_clean_zero,
  S.alert_raw_created_at_invalid,
  S.alert_clean_created_at_null
);