-- ops//archive/22_ops_customer_data_quality_alerts_daily_v2_avg.sql
-- Purpose:
--   customer_data_quality_daily から「過去3日平均」との乖離で異常検知し、
--   ops.customer_data_quality_alerts_daily に日次UPSERTする。
-- Notes:
--   - 指標は横持ち→縦持ち化し、共通ロジックで処理
--   - avg_3d は「当日を含まない」過去3日（1〜3日前）の平均
--   - 累計（mart_latest_cumulative）は「減少のみ異常（前日比）」を維持
--   - 同日再実行は MERGE により上書き

DECLARE v_run_date DATE DEFAULT CURRENT_DATE("Asia/Tokyo");
DECLARE rule_version STRING DEFAULT "v2_avg3d";

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
    -- 平均算出のために「当日 + 過去3日 + 前日」を含める
    -- ここは最低でも v_run_date - 3 〜 v_run_date を取ればOK
    WHERE run_date BETWEEN DATE_SUB(v_run_date, INTERVAL 3 DAY) AND v_run_date
  ),

  -- 横持ち → 縦持ち（指標追加しやすい）
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

  -- 前日値 + 過去3日平均（当日を含まない）を付与
  calc AS (
    SELECT
      run_date,
      metric_name,
      metric_value,

      LAG(metric_value) OVER (
        PARTITION BY metric_name
        ORDER BY run_date
      ) AS prev_value,

      AVG(metric_value) OVER (
        PARTITION BY metric_name
        ORDER BY run_date
        ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
      ) AS avg_3d
    FROM metrics
  ),

  -- 当日分だけに絞ってスコア計算
  scored AS (
    SELECT
      run_date,
      metric_name,
      metric_value,
      prev_value,
      avg_3d,

      -- 前日比（累計の減少判定に使う）
      (metric_value - prev_value) AS delta_value,
      SAFE_DIVIDE(metric_value - prev_value, NULLIF(prev_value, 0)) AS change_rate,

      -- 平均比較（v2の核）
      (metric_value - avg_3d) AS delta_from_avg,
      SAFE_DIVIDE(metric_value - avg_3d, NULLIF(avg_3d, 0)) AS rate_from_avg,

      CASE
        WHEN metric_name IN ("raw_row_count","clean_row_count") THEN "row"
        WHEN metric_name = "mart_latest_cumulative" THEN "cumulative"
        ELSE "error_or_kpi"
      END AS metric_type
    FROM calc
    WHERE run_date = v_run_date
  ),

  -- 異常判定（平均比較）
  judged AS (
    SELECT
      run_date,
      metric_name,
      metric_value,
      prev_value,
      avg_3d,
      delta_value,
      change_rate,
      delta_from_avg,
      rate_from_avg,

      CASE
        WHEN avg_3d IS NULL THEN FALSE
        WHEN metric_name = "mart_latest_cumulative" THEN FALSE
        WHEN metric_type = "row" THEN (
          delta_from_avg >= row_abs
          OR SAFE_DIVIDE(delta_from_avg, NULLIF(avg_3d, 0)) >= row_rate
        )
        ELSE (
          delta_from_avg >= err_abs
          OR SAFE_DIVIDE(delta_from_avg, NULLIF(avg_3d, 0)) >= err_rate
        )
      END AS is_spike,

      CASE
        WHEN avg_3d IS NULL THEN FALSE
        WHEN metric_name = "mart_latest_cumulative" THEN (delta_value < 0)
        WHEN metric_type = "row" THEN (
          (-delta_from_avg) >= row_abs
          OR SAFE_DIVIDE(-delta_from_avg, NULLIF(avg_3d, 0)) >= row_rate
        )
        ELSE (
          (-delta_from_avg) >= err_abs
          OR SAFE_DIVIDE(-delta_from_avg, NULLIF(avg_3d, 0)) >= err_rate
        )
      END AS is_drop
    FROM scored
  )

  SELECT
    run_date,
    metric_name,
    metric_value,
    prev_value,
    avg_3d,
    delta_from_avg,
    rate_from_avg,
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
  avg_3d = S.avg_3d,
  delta_from_avg = S.delta_from_avg,
  rate_from_avg = S.rate_from_avg,
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
  INSERT (
    run_date, metric_name,
    metric_value, prev_value,
    avg_3d, delta_from_avg, rate_from_avg,
    delta_value, change_rate,
    is_spike, is_drop, is_anomaly,
    rule_version, threshold_abs, threshold_rate, created_at
  )
  VALUES (
    S.run_date, S.metric_name,
    S.metric_value, S.prev_value,
    S.avg_3d, S.delta_from_avg, S.rate_from_avg,
    S.delta_value, S.change_rate,
    S.is_spike, S.is_drop, S.is_anomaly,
    S.rule_version, S.threshold_abs, S.threshold_rate, S.created_at
  );