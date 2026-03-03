-- ops/21_ops_customer_data_quality_alerts_daily.sql
-- Purpose:
--   customer_data_quality_daily（メトリクス）から前日比を算出し、
--   急増/急減（差分・変動率）を判定して alerts テーブルに日次UPSERTする。
-- Notes:
--   - run_date（Asia/Tokyo）単位で同日再実行は上書き（MERGE）
--   - 累計指標（mart_latest_cumulative）は「減少のみ異常」

DECLARE v_run_date DATE DEFAULT CURRENT_DATE("Asia/Tokyo");
DECLARE rule_version STRING DEFAULT "v1.1";

-- row_count系（規模に合わせて調整）
DECLARE row_abs INT64 DEFAULT 50;
DECLARE row_rate FLOAT64 DEFAULT 0.3;

-- error/KPI系（小さめに）
DECLARE err_abs INT64 DEFAULT 5;
DECLARE err_rate FLOAT64 DEFAULT 1.0;

MERGE `ec-data-platform.ops.customer_data_quality_alerts_daily` T
USING (
  WITH base AS (
    SELECT *
    FROM `ec-data-platform.ops.customer_data_quality_daily`
    WHERE run_date IN (DATE_SUB(v_run_date, INTERVAL 1 DAY), v_run_date)
  ),

  -- 横持ち → 縦持ち（指標を増やしやすくする）
  metrics AS (
    SELECT
      run_date,
      m.metric_name,
      m.metric_value
    FROM base,
    UNNEST([
      STRUCT("raw_row_count" AS metric_name, raw_row_count AS metric_value),
      STRUCT("clean_row_count" AS metric_name, clean_row_count AS metric_value),

      STRUCT("customer_id_empty_count" AS metric_name, customer_id_empty_count AS metric_value),
      STRUCT("raw_duplicate_customer_id_rows" AS metric_name, raw_duplicate_customer_id_rows AS metric_value),
      STRUCT("raw_created_at_invalid_count" AS metric_name, raw_created_at_invalid_count AS metric_value),
      STRUCT("clean_created_at_null_count" AS metric_name, clean_created_at_null_count AS metric_value),

      STRUCT("mart_daily_signups_sum_90d" AS metric_name, mart_daily_signups_sum_90d AS metric_value),
      STRUCT("mart_latest_cumulative" AS metric_name, mart_latest_cumulative AS metric_value)
    ]) AS m
  ),

  -- 指標ごとに前日値を付与
  calc AS (
    SELECT
      run_date,
      metric_name,
      metric_value,
      LAG(metric_value) OVER (PARTITION BY metric_name ORDER BY run_date) AS prev_value
    FROM metrics
  ),

  -- 差分・変動率と指標タイプを付与（今日分のみ）
  scored AS (
    SELECT
      run_date,
      metric_name,
      metric_value,
      prev_value,
      (metric_value - prev_value) AS delta_value,
      SAFE_DIVIDE(metric_value - prev_value, NULLIF(prev_value, 0)) AS change_rate,
      CASE
        WHEN metric_name IN ("raw_row_count","clean_row_count") THEN "row"
        WHEN metric_name = "mart_latest_cumulative" THEN "cumulative"
        ELSE "error_or_kpi"
      END AS metric_type
    FROM calc
    WHERE run_date = v_run_date
  ),

  -- 急増/急減判定
  judged AS (
    SELECT
      run_date,
      metric_name,
      metric_value,
      prev_value,
      delta_value,
      change_rate,

      CASE
        WHEN prev_value IS NULL THEN FALSE
        WHEN metric_type = "cumulative" THEN FALSE
        WHEN metric_type = "row" THEN (
          delta_value >= row_abs OR SAFE_DIVIDE(delta_value, NULLIF(prev_value, 0)) >= row_rate
        )
        ELSE (
          delta_value >= err_abs OR SAFE_DIVIDE(delta_value, NULLIF(prev_value, 0)) >= err_rate
        )
      END AS is_spike,

      CASE
        WHEN prev_value IS NULL THEN FALSE
        WHEN metric_type = "cumulative" THEN (delta_value < 0)
        WHEN metric_type = "row" THEN (
          (-delta_value) >= row_abs OR SAFE_DIVIDE(-delta_value, NULLIF(prev_value, 0)) >= row_rate
        )
        ELSE (
          (-delta_value) >= err_abs OR SAFE_DIVIDE(-delta_value, NULLIF(prev_value, 0)) >= err_rate
        )
      END AS is_drop
    FROM scored
  )

  SELECT
    run_date,
    metric_name,
    metric_value,
    prev_value,
    delta_value,
    change_rate,
    is_spike,
    is_drop,
    (is_spike OR is_drop) AS is_anomaly,
    rule_version AS rule_version,
    row_abs AS threshold_abs,
    row_rate AS threshold_rate,
    CURRENT_TIMESTAMP() AS created_at
  FROM judged
) S
ON  T.run_date = S.run_date
AND T.metric_name = S.metric_name
WHEN MATCHED THEN UPDATE SET
  metric_value = S.metric_value,
  prev_value = S.prev_value,
  delta_value = S.delta_value,
  change_rate = S.change_rate,
  is_spike = S.is_spike,
  is_drop = S.is_drop,
  is_anomaly = S.is_anomaly,
  rule_version = S.rule_version,
  threshold_abs = S.threshold_abs,
  threshold_rate = S.threshold_rate,
  created_at = S.created_at
WHEN NOT MATCHED THEN
  INSERT (run_date, metric_name, metric_value, prev_value, delta_value, change_rate,
          is_spike, is_drop, is_anomaly, rule_version, threshold_abs, threshold_rate, created_at)
  VALUES (S.run_date, S.metric_name, S.metric_value, S.prev_value, S.delta_value, S.change_rate,
          S.is_spike, S.is_drop, S.is_anomaly, S.rule_version, S.threshold_abs, S.threshold_rate, S.created_at);