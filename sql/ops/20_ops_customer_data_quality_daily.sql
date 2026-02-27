-- ops/customer_data_quality_daily.sql
-- Purpose:
--   customersパイプライン（raw -> clean -> mart）の
--   日次データ品質スナップショットを記録する
-- Notes:
--   run_date（Asia/Tokyo）ごとに1行を保持する
--   本SQLは実行日分のデータを再作成する

DECLARE run_date DATE DEFAULT CURRENT_DATE("Asia/Tokyo");

CREATE OR REPLACE TABLE `ec-data-platform.ops.customer_data_quality_daily`
PARTITION BY run_date
AS
WITH
raw_base AS (
  SELECT
    customer_id,
    email,
    created_at
  FROM `ec-data-platform.raw.raw_customers`
),
raw_metrics AS (
  SELECT
    COUNT(*) AS raw_row_count,
    COUNTIF(customer_id IS NULL OR TRIM(customer_id) = "") AS raw_customer_id_empty_count,

    -- created_at='created_at' (CSVヘッダー混入) やパース不能を拾う
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
    -- 重複しているcustomer_idの種類数
    COUNTIF(cnt > 1) AS raw_duplicate_customer_id_keys,
    -- 重複行の総数（例：Aが3件なら 3-1=2 を足す）
    SUM(IF(cnt > 1, cnt - 1, 0)) AS raw_duplicate_customer_id_rows
  FROM (
    SELECT customer_id, COUNT(*) AS cnt
    FROM raw_base
    WHERE customer_id IS NOT NULL AND TRIM(customer_id) != ""
    GROUP BY customer_id
  )
),
clean_base AS (
  SELECT
    customer_id,
    created_at
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
    -- 直近90日の新規合計（急落/急増を拾う）
    (SELECT IFNULL(SUM(new_customers), 0)
     FROM `ec-data-platform.mart.customer_daily_signups`
     WHERE signup_date >= DATE_SUB(run_date, INTERVAL 90 DAY)
    ) AS mart_daily_signups_sum_90d,

    -- 最新の累計（0に戻る/不自然な減少を拾う）
    (SELECT IFNULL(MAX(new_customers), 0)
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

  -- “軽い異常フラグ”（最初は雑でOK。後で閾値を育てる）
  IF(raw_metrics.raw_row_count = 0, TRUE, FALSE) AS alert_raw_zero,
  IF(clean_metrics.clean_row_count = 0, TRUE, FALSE) AS alert_clean_zero,
  IF(raw_metrics.raw_created_at_invalid_count > 0, TRUE, FALSE) AS alert_raw_created_at_invalid,
  IF(clean_metrics.clean_created_at_null_count > 0, TRUE, FALSE) AS alert_clean_created_at_null
FROM raw_metrics
CROSS JOIN raw_dupe
CROSS JOIN clean_metrics
CROSS JOIN mart_metrics;