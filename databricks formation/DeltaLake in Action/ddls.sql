-- Databricks notebook source
CREATE TABLE IF NOT EXISTS bronze_dev.external_tables.client_brz_ext (
  id          STRING,
  name        STRING,
  last_name   STRING,
  email       STRING,
  birth_date  STRING,
  country     STRING,
  role        STRING,
  ingest_timestamp  TIMESTAMP,
  ingest_date       DATE
)
USING DELTA
LOCATION 's3://lakehouse-datawizard-dev/bronze-dev/external_tables/client_brz_ext';

-- COMMAND ----------

describe extended bronze_dev.external_tables.client_brz_ext

-- COMMAND ----------



-- COMMAND ----------

CREATE TABLE IF NOT EXISTS bronze_dev.external_tables.client_brz_ext_tmp (
  id          STRING,
  name        STRING,
  last_name   STRING,
  email       STRING,
  birth_date  STRING,
  country     STRING,
  role        STRING,
  ingest_timestamp  TIMESTAMP,
  ingest_date       DATE
)
USING DELTA
LOCATION 's3://lakehouse-datawizard-dev/bronze-dev/external_tables/client_brz_ext_tmp';

-- COMMAND ----------

describe extended bronze_dev.external_tables.client_brz_ext_tmp

-- COMMAND ----------


