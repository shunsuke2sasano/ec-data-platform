--mart/12_mart_customer_monthly_signups.sql
P--urpose:
--  mart.customer_monthly_signups を作成する（月次の新規顧客数）

--Definition:
--  - customer_id ごとの初回登録日（MIN(created_at)）を月単位に丸めて集計
--  - 月は Asia/Tokyo の日付基準で算出

--Grain:
--  - 1行 = 1ヶ月（signup_month）

CREATE OR REPLACE TABLE `ec-data-platform.mart.customer_monthly_signups` AS
WITH per_customer AS (
  SELECT
    customer_id,
    MIN(created_at) AS first_created_at
  FROM `ec-data-platform.clean.customers`
  WHERE created_at IS NOT NULL
  GROUP BY customer_id
)
SELECT
  DATE_TRUNC(DATE(first_created_at, "Asia/Tokyo"), MONTH) AS signup_month,
  COUNT(*) AS new_customers
FROM per_customer
GROUP BY signup_month
ORDER BY signup_month;