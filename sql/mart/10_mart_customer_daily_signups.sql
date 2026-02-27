--mart/10_mart_customer_daily_signups.sql
--Purpose:
--  mart.customer_daily_signups を作成する（日別の新規顧客数）

--Definition:
--  - 「新規顧客」= customer_id ごとの初回登録日（MIN(created_at)）
--  - 日付は Asia/Tokyo で切り出す（UTCズレ防止）

--Grain:
--  - 1行 = 1日（signup_date）

--Notes:
--  - created_at が NULL の行は除外（分析不能のため）


CREATE OR REPLACE TABLE `ec-data-platform.mart.customer_daily_signups` AS
WITH per_customer AS (
  SELECT
    customer_id,
    MIN(created_at) AS first_created_at
  FROM `ec-data-platform.clean.customers`  
  WHERE created_at IS NOT NULL
  GROUP BY customer_id
)
SELECT
  DATE(first_created_at, "Asia/Tokyo") AS signup_date,
  COUNT(*) AS new_customers
FROM per_customer
GROUP BY signup_date
ORDER BY signup_date;