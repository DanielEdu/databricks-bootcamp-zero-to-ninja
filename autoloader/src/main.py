# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesta Landing a Bronze con Auto Loader
# MAGIC
# MAGIC Este proyecto implementa la ingesta de datos desde la capa **Landing** hacia **Bronze** usando **Databricks Auto Loader**.
# MAGIC
# MAGIC La solución está estructurada como un proyecto tipo **Databricks Asset Bundle**.

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date, current_timestamp, struct

# COMMAND ----------

table_name = "clients"

# COMMAND ----------

checkpoint_path     = f"s3://lakehouse-datawizard-dev/bronze-dev/_pipelines/{table_name}/_checkpoint/16"
schema_path         = f"s3://lakehouse-datawizard-dev/bronze-dev/_pipelines/{table_name}/_schema/"

# COMMAND ----------

# DBTITLE 1,Leer archivos Parquet desde S3
df_stream = (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .option("cloudFiles.schemaLocation", schema_path)  # ← Esta línea es clave
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .load("s3://lakehouse-datawizard-dev/landing-dev/clients/")
)

df_stream = (
   df_stream.select(
      "*",
      struct(
         col("_metadata.file_name").alias("file_name"),
         col("_metadata.file_path").alias("file_path"),
         col("_metadata.file_size").cast("integer").alias("file_size"),
         col("_metadata.file_modification_time").alias("modification_ts"),
      ).alias("metadata"),
      current_timestamp().alias("ingestion_ts"),
      current_date().alias("ingestion_dt")

   )
   .drop("_rescued_data")
)

# COMMAND ----------

# MAGIC %skip
# MAGIC display(df_stream, checkpointLocation = checkpoint_path)

# COMMAND ----------

# DBTITLE 1,Crear tabla Delta clientes bronze con liquid clustering
# MAGIC %sql
# MAGIC create catalog if not exists bronze_dev;
# MAGIC create schema if not exists bronze_dev.erp;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze_dev.erp.clients (
# MAGIC   id          STRING,
# MAGIC   name        STRING,
# MAGIC   last_name   STRING,
# MAGIC   email       STRING,
# MAGIC   birth_date  STRING,
# MAGIC   country     STRING,
# MAGIC   role        STRING,
# MAGIC   metadata STRUCT <
# MAGIC     file_name       : STRING,
# MAGIC     file_path       : STRING,
# MAGIC     file_size       : INT,
# MAGIC     modification_ts : TIMESTAMP
# MAGIC   >,
# MAGIC   ingestion_ts  TIMESTAMP  NOT NULL,
# MAGIC   ingestion_dt  DATE       NOT NULL
# MAGIC )
# MAGIC CLUSTER BY (ingestion_dt)

# COMMAND ----------


