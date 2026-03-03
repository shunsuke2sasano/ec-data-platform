-- ops/19_ops_alert_thresholds.sql
-- Purpose:
--   異常検知の閾値を管理する設定テーブル。
--   SQLロジックと閾値を分離し、設定駆動型アラートを実現する。
-- Notes:
--   - metric_name は alerts SQL 内の STRUCT 名と一致させる
--   - is_enabled = TRUE の指標のみ判定対象
--   - 同じmetric_nameはMERGEで上書き（再実行安全）

CREATE TABLE IF NOT EXISTS `ec-data-platform.ops.alert_thresholds` (
  metric_name STRING,
  metric_type STRING,        -- row / error_or_kpi / cumulative
  threshold_abs INT64,       -- 件数ベース閾値
  threshold_rate FLOAT64,    -- 割合ベース閾値
  is_enabled BOOL,
  updated_at TIMESTAMP
);

MERGE `ec-data-platform.ops.alert_thresholds` T
USING (
  SELECT * FROM UNNEST([
    -- 件数系
    STRUCT(
      "raw_row_count" AS metric_name,
      "row" AS metric_type,
      500 AS threshold_abs,
      0.2 AS threshold_rate,
      TRUE AS is_enabled,
      CURRENT_TIMESTAMP() AS updated_at
    ),

    STRUCT(
      "clean_row_count",
      "row",
      500,
      0.2,
      TRUE,
      CURRENT_TIMESTAMP()
    ),

    -- エラー系
    STRUCT(
      "customer_id_empty_count",
      "error_or_kpi",
      10,
      0.5,
      TRUE,
      CURRENT_TIMESTAMP()
    ),

    STRUCT(
      "raw_duplicate_customer_id_rows",
      "error_or_kpi",
      10,
      0.5,
      TRUE,
      CURRENT_TIMESTAMP()
    ),

    STRUCT(
      "raw_created_at_invalid_count",
      "error_or_kpi",
      5,
      0.5,
      TRUE,
      CURRENT_TIMESTAMP()
    ),

    STRUCT(
      "clean_created_at_null_count",
      "error_or_kpi",
      5,
      0.5,
      TRUE,
      CURRENT_TIMESTAMP()
    ),

    -- KPI系
    STRUCT(
      "mart_daily_signups_sum_90d",
      "error_or_kpi",
      100,
      0.3,
      TRUE,
      CURRENT_TIMESTAMP()
    ),

    -- 累計系（減少のみ異常）
    STRUCT(
      "mart_latest_cumulative",
      "cumulative",
      0,
      0.0,
      TRUE,
      CURRENT_TIMESTAMP()
    )
  ])
) S
ON T.metric_name = S.metric_name
WHEN MATCHED THEN UPDATE SET
  metric_type = S.metric_type,
  threshold_abs = S.threshold_abs,
  threshold_rate = S.threshold_rate,
  is_enabled = S.is_enabled,
  updated_at = S.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    metric_name,
    metric_type,
    threshold_abs,
    threshold_rate,
    is_enabled,
    updated_at
  )
  VALUES (
    S.metric_name,
    S.metric_type,
    S.threshold_abs,
    S.threshold_rate,
    S.is_enabled,
    S.updated_at
  );