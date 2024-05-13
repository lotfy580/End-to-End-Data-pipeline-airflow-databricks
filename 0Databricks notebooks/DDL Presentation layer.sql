-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Presentation Schema

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS presentation.ct;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS presentation.ct.dim_date AS
SELECT
    replace(calendar_date,'-','') as date_id,
    calendar_date AS date_full,
    YEAR(calendar_date) AS year,
    QUARTER(calendar_date) AS quarter,
    MONTH(calendar_date) AS month,
    weekofyear(calendar_date) AS week,
    DAY(calendar_date) AS day,
    WEEKDAY(calendar_date) AS day_of_week,
    date_format(calendar_date,'E') as day_name,
    date_format(calendar_date,'MMMM') as month_name,
    CASE WHEN dayofweek(calendar_date) IN (1, 7) THEN 0 ELSE 1 END AS is_weekday,
    CASE WHEN year(calendar_date) % 4 = 0 THEN 1 ELSE 0 END AS is_leapyear
FROM (
        SELECT explode(sequence(DATE'1962-01-01', DATE'2030-12-31', INTERVAL 1 DAY)) as  calendar_date
    ) AS dates

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS presentation.ct.dim_customer (
  customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_bk string NOT NULL,
  name varchar(50) NOT NULL,
  user_name varchar(100),
  address varchar(255),
  birth_date date,
  email varchar(100),
  active_customer BOOLEAN,
  bronze_tier BOOLEAN,
  silver_tier BOOLEAN,
  gold_tier BOOLEAN,
  platinum_tier BOOLEAN,
  24_hour_dedicated_line_benefit BOOLEAN,
  concert_ticket_benefit BOOLEAN,
  travel_insurance_benefit BOOLEAN,
  financial_planning_assistance_benefit BOOLEAN,
  shopping_discounts_benefit BOOLEAN,
  sports_tickets_benefit BOOLEAN,
  concierge_service_benefit BOOLEAN,
  car_rental_benefit BOOLEAN,
  airline_lounge_access_benefit BOOLEAN,
  dedicated_account_representative BOOLEAN,
  start_date date,
  end_date date,
  is_current BOOLEAN,
  source_system varchar(20)
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS presentation.ct.dim_account(
  account_sk BIGINT GENERATED ALWAYS AS IDENTITY,
  account_id int NOT NULL,
  limit int,
  has_investment_stock boolean,
  has_brokerage boolean,
  has_commodity boolean,
  has_investment_fund boolean,
  has_currency_service boolean,
  has_dervatives boolean,
  start_date date,
  end_date date,
  is_current boolean,
  source_system varchar(20)
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS presentation.ct.dim_transaction_details(
  transaction_details_pk BIGINT GENERATED ALWAYS AS IDENTITY,
  transaction_type varchar(10),
  symbol varchar(10),
  source_system varchar(20)
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS presentation.ct.fact_transaction(
  fact_transactions_pk BIGINT GENERATED ALWAYS AS IDENTITY,
  transaction_bk bigint NOT NULL,
  customer_fk int NOT NULL,
  account_fk int NOT NULL,
  trancation_details_fk int,
  transaction_date_fk int NOT NULL,
  account_bk int NOT NULL,
  customer_bk string NOT NULL,
  amount int,
  unit_price decimal(10, 2),
  create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported'
  );
