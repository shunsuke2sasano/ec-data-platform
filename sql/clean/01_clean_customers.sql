-- Layer: clean
-- Purpose: raw.raw_customersの重複削除と型変換
-- Rule:
-- 1. customer_id単位で最新created_atを採用
-- 2. created_atはTIMESTAMPへ変換
-- 3. NULLはSAFEで回避

CREATE OR REPLACE TABLE `clean.customers` AS
WITH src AS (
  SELECT
    customer_id,
    LOWER(TRIM(email)) AS email,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', created_at) AS created_at_ts
  FROM `raw.raw_customers`
  WHERE created_at != 'created_at'   -- ← ヘッダー除外
),
dedup AS (
  SELECT
    customer_id,
    email,
    created_at_ts,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY created_at_ts DESC, email
    ) AS rn
  FROM src
)
SELECT
  customer_id,
  email,
  created_at_ts AS created_at
FROM dedup
WHERE rn = 1;