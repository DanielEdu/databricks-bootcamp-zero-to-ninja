# Databricks notebook source
# MAGIC %sql
# MAGIC select * from bronze_dev.external_tables.client_brz_ext version as of 4

# COMMAND ----------

spark.sql("RESTORE TABLE bronze_dev.external_tables.client_brz_ext TO VERSION AS OF 4")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table bronze_dev.external_tables.client_brz_ext

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs ls s3://lakehouse-datawizard-dev/bronze-dev/external_tables/client_brz_ext

# COMMAND ----------

file = dbutils.fs.ls("s3://lakehouse-datawizard-dev/bronze-dev/external_tables/client_brz_ext/_delta_log/")

display(file)

# COMMAND ----------

# MAGIC %md
# MAGIC ``` text
# MAGIC _delta_log/
# MAGIC    ├── 00000000000000000000.json  ← versión 0
# MAGIC    ├── 00000000000000000001.json  ← versión 1
# MAGIC    ├── 00000000000000000000.crc   ← checksum
# MAGIC    └── _staged_commits/          ← commits temporales
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, date_format, current_date

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze_dev.external_tables.client_brz_ext

# COMMAND ----------

clients_df = (
  spark.read.format("csv")
  .option("header", True)
  .option("delimiter", ",")
  .load("s3://lakehouse-datawizard-dev/landing-dev/clients/")
).withColumn("ingest_timestamp", current_timestamp()).withColumn("ingest_date", current_date())

clients_df.display()


# COMMAND ----------

clients_df.writeTo("bronze_dev.external_tables.client_brz_ext").append()
