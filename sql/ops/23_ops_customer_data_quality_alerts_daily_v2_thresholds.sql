-- ops/23_ops_customer_data_quality_alerts_daily_v2_thresholds.sql
-- Purpose:
--   customer_data_quality_daily の各メトリクスを「過去3日平均」と比較し、
--   閾値は ops.alert_thresholds（設定テーブル）から読み込んで異常判定する。
-- Notes:
--   - 指標は横持ち→縦持ち化（metric_name/metric_value）
--   - avg_3d は当日を含まない過去3日（1〜3日前）平均
--   - cumulative（累計）は「減少のみ異常」（前日比で判定）
--   - 閾値テーブルで is_enabled=TRUE の指標だけ判定対象
--   - 同日再実行は MERGE により上書き（UPSERT）

DECLARE v_run_date DATE DEFAULT CURRENT_DATE("Asia/Tokyo");
DECLARE rule_version STRING DEFAULT "v2_avg3d_thresholds";

MERGE `ec-data-platform.ops.customer_data_quality_alerts_daily` T
USING (
  WITH base AS (
    SELECT *
    FROM `ec-data-platform.ops.customer_data_quality_daily`
    WHERE run_date BETWEEN DATE_SUB(v_run_date, INTERVAL 3 DAY) AND v_run_date
  ),

  -- 横持ち（列いっぱい）→ 縦持ち（行）に変換：指標追加が簡単になる
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

  -- 前日値（LAG）と過去3日平均（当日除く）を付与
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

  -- 今日分だけ抽出し、差分/割合を計算 + 閾値テーブルをJOIN
  scored AS (
    SELECT
      c.run_date,
      c.metric_name,
      c.metric_value,
      c.prev_value,
      c.avg_3d,

      -- 前日比（累計の減少判定に利用）
      (c.metric_value - c.prev_value) AS delta_value,
      SAFE_DIVIDE(c.metric_value - c.prev_value, NULLIF(c.prev_value, 0)) AS change_rate,

      -- 平均比較（v2の核）
      (c.metric_value - c.avg_3d) AS delta_from_avg,
      SAFE_DIVIDE(c.metric_value - c.avg_3d, NULLIF(c.avg_3d, 0)) AS rate_from_avg,

      -- 閾値テーブル
      th.metric_type,
      th.threshold_abs,
      th.threshold_rate,
      th.is_enabled
    FROM calc c
    JOIN `ec-data-platform.ops.alert_thresholds` th
      ON th.metric_name = c.metric_name
    WHERE c.run_date = v_run_date
      AND th.is_enabled = TRUE
  ),

  -- 異常判定（NULL/0ガード込みで必ずBOOLになる）
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
      metric_type,
      threshold_abs,
      threshold_rate,

      -- spike（急増）
      CASE
        WHEN avg_3d IS NULL THEN FALSE
        WHEN metric_type = "cumulative" THEN FALSE
        WHEN avg_3d = 0 THEN (delta_from_avg >= threshold_abs)
        ELSE (
          delta_from_avg >= threshold_abs
          OR COALESCE(rate_from_avg, 0) >= threshold_rate
        )
      END AS is_spike,

      -- drop（急減）
      CASE
        WHEN avg_3d IS NULL THEN FALSE
        WHEN metric_type = "cumulative" THEN (delta_value < 0)
        WHEN avg_3d = 0 THEN ((-delta_from_avg) >= threshold_abs)
        ELSE (
          (-delta_from_avg) >= threshold_abs
          OR COALESCE(-rate_from_avg, 0) >= threshold_rate
        )
      END AS is_drop
    FROM scored
  )

  SELECT
    run_date,
    metric_name,
    metric_value,
    prev_value,

    -- v2保存
    avg_3d,
    delta_from_avg,
    rate_from_avg,

    -- 参考（運用で便利）
    delta_value,
    change_rate,

    is_spike,
    is_drop,
    (is_spike OR is_drop) AS is_anomaly,

    rule_version AS rule_version,

    -- 適用した閾値も保存（再現性と説明責任）
    threshold_abs AS threshold_abs,
    threshold_rate AS threshold_rate,

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