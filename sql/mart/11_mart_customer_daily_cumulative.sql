/*
File: 11_mart_customer_daily_cumulative.sql
Purpose:
  mart.customer_daily_cumulative を作成する（日別新規 + 累計顧客数）

Definition:
  - new_customers: 日別新規顧客数
  - cumulative_customers: 開始日からの累計（window関数）

Design:
  - 欠損日を0埋め（カレンダー生成）して、BIで扱いやすくする

Grain:
  - 1行 = 1日（signup_date）
*/

CREATE OR REPLACE TABLE `ec-data-platform.mart.customer_daily_cumulative` AS
WITH daily AS (
  SELECT
    signup_date,
    new_customers
  FROM `ec-data-platform.mart.customer_daily_signups`
),
calendar AS (
  SELECT d AS signup_date
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      (SELECT MIN(signup_date) FROM daily),
      (SELECT MAX(signup_date) FROM daily)
    )
  ) AS d
),
filled AS (
  SELECT
    c.signup_date,
    COALESCE(d.new_customers, 0) AS new_customers
  FROM calendar c
  LEFT JOIN daily d
  USING (signup_date)
)
SELECT
  signup_date,
  new_customers,
  SUM(new_customers) OVER (ORDER BY signup_date) AS cumulative_customers
FROM filled
ORDER BY signup_date;