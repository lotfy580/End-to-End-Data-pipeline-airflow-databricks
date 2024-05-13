-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Processing Schema

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS processed.ct;


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS processed.ct.customer (
  id string PRIMARY KEY,
  user_name string NOT NULL,
  name string NOT NULL,
  address string,
  birth_date date NOT NULL,
  email string NOT NULL,
  active_customer BOOLEAN,
  account_id int NOT NULL,
  tier string NOT NULL,
  benefis array<string>,
  tier_active boolean,
  ingest_date  TIMESTAMP NOT NULL,
  create_date TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported'
  );

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS processed.ct.account (
  id string NOT NULL,
  account_id int PRIMARY KEY,
  limit int NOT NULL,
  products array<string>,
  ingest_date timestamp NOT NULL,
  create_date timestamp DEFAULT current_timestamp()
)
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported'
  );

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS processed.ct.transaction (
  transaction_id bigint NOT NULL,
  account_id int NOT NULL,
  transaction_date date NOT NULL,
  transaction_type string NOT NULL,
  symbol string,
  amount int,
  unit_price decimal(10, 2),
  total_price decimal(10, 2),
  bucket_start_date date,
  bucker_end_date date,
  transactions_count int,
  ingest_date timestamp NOT NULL,
  create_date timestamp DEFAULT current_timestamp()
)
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported'
  );

