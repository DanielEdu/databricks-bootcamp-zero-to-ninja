-- Databricks notebook source
CREATE TABLE bronze_dev.bakehouse.customer_brz_ext (
  customerID    STRING,
  first_name    STRING,
  last_name     STRING,
  email_address STRING,
  phone_number  STRING,
  address       STRING,
  city          STRING,
  state         STRING,
  country       STRING,
  continent     STRING,
  postal_zip_code   STRING,
  gender            STRING,
  ingest_timestamp  TIMESTAMP,
  ingest_date       DATE
)
USING DELTA
LOCATION 's3://lakehouse-datawizard-dev/bronze-dev/customer_ext/';

-- COMMAND ----------

CREATE TABLE bronze_dev.bakehouse_ext.customer_brz_ext (
  customerID    STRING,
  first_name    STRING,
  last_name     STRING,
  email_address STRING,
  phone_number  STRING,
  address       STRING,
  city          STRING,
  state         STRING,
  country       STRING,
  continent     STRING,
  postal_zip_code   STRING,
  gender            STRING,
  ingest_timestamp  TIMESTAMP,
  ingest_date       DATE
)
USING DELTA
LOCATION 's3://lakehouse-datawizard-dev/bronze-dev/bakehouse_ext/customer_brz_ext';

-- COMMAND ----------

CREATE CATALOG landing_mng;

-- COMMAND ----------

CREATE TABLE bronze_dev.bakehouse.customer_brz(
  customerID    STRING,
  first_name    STRING,
  last_name     STRING,
  email_address STRING,
  phone_number  STRING,
  address       STRING,
  city          STRING,
  state         STRING,
  country       STRING,
  continent     STRING,
  postal_zip_code   STRING,
  gender            STRING,
  ingest_timestamp  TIMESTAMP,
  ingest_date       DATE
)

-- COMMAND ----------

describe extended bronze_dev.bakehouse.customer_brz

-- COMMAND ----------


