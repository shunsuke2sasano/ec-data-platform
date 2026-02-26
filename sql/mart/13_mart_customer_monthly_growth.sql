/*
File: 13_mart_customer_monthly_growth.sql
Purpose:
  mart.customer_monthly_growth を作成する（月次の新規/累計/前月比）

Metrics:
  - new_customers: 月次新規
  - cumulative_customers: 累計
  - prev_month_new_customers: 前月新規
  - mom_growth_rate: 前月比成長率（SAFE_DIVIDEで0割回避）

Grain:
  - 1行 = 1ヶ月（signup_month）
*/

CREATE OR REPLACE TABLE `ec-data-platform.mart.customer_monthly_growth` AS
WITH m AS (
  SELECT
    signup_month,
    new_customers
  FROM `ec-data-platform.mart.customer_monthly_signups`
)
SELECT
  signup_month,
  new_customers,
  SUM(new_customers) OVER (ORDER BY signup_month) AS cumulative_customers,
  LAG(new_customers) OVER (ORDER BY signup_month) AS prev_month_new_customers,
  SAFE_DIVIDE(
    new_customers - LAG(new_customers) OVER (ORDER BY signup_month),
    LAG(new_customers) OVER (ORDER BY signup_month)
  ) AS mom_growth_rate
FROM m
ORDER BY signup_month;