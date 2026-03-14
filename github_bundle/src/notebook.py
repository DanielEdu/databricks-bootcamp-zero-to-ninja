# Databricks notebook source
# MAGIC %md
# MAGIC # Autoloader: Ingesta de Clientes (Landing → Bronze)
# MAGIC
# MAGIC Este notebook lee archivos desde S3 usando **PySpark Autoloader** (Structured Streaming),
# MAGIC aplica **schema evolution** automática y escribe los datos en la tabla Delta
# MAGIC `bronze_dev.default.clients` del catálogo Unity Catalog.
# MAGIC
# MAGIC | Parámetro | Valor |
# MAGIC |---|---|
# MAGIC | Fuente (Landing) | `s3://lakehouse-datawizard-dev/landing-dev/clients/` |
# MAGIC | Checkpoint | `s3://lakehouse-datawizard-dev/bronze-dev/_pipelines/clients/_checkpoint/` |
# MAGIC | Schema Location | `s3://lakehouse-datawizard-dev/bronze-dev/_pipelines/clients_brz/_schema/` |
# MAGIC | Tabla destino | `bronze_dev.default.clients` |

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parámetros

# COMMAND ----------

SOURCE_PATH      = "s3://lakehouse-datawizard-dev/landing-dev/clients/"
CHECKPOINT_PATH  = "s3://lakehouse-datawizard-dev/bronze-dev/_pipelines/clients/_checkpoint/"
SCHEMA_PATH      = "s3://lakehouse-datawizard-dev/bronze-dev/_pipelines/clients_brz/_schema/"
TARGET_TABLE     = "bronze_dev.default.clients"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lectura con Autoloader
# MAGIC
# MAGIC Se utiliza `cloudFiles` como formato de fuente para habilitar Autoloader.
# MAGIC - `cloudFiles.format`: formato de los archivos en la zona landing (ej. `json`, `csv`, `parquet`).
# MAGIC - `cloudFiles.schemaLocation`: directorio donde Autoloader persiste y evoluciona el schema inferido.
# MAGIC - `cloudFiles.inferColumnTypes`: infiere tipos de columna más precisos en lugar de usar `string` por defecto.
# MAGIC - `mergeSchema`: habilita **schema evolution** al hacer el write, permitiendo agregar nuevas columnas automáticamente.

# COMMAND ----------

df_raw = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.includeExistingFiles", "true")
        .load(SOURCE_PATH)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Columnas técnicas
# MAGIC
# MAGIC Se agregan columnas de auditoría y metadata para trazabilidad de la ingesta:
# MAGIC
# MAGIC | Columna | Tipo | Descripción |
# MAGIC |---|---|---|
# MAGIC | `_ingested_at` | `timestamp` | Momento exacto en que el registro fue procesado por el pipeline |
# MAGIC | `_ingested_date` | `date` | Fecha de ingesta (útil para particionamiento y filtros) |
# MAGIC | `_source_file` | `string` | Ruta completa del archivo fuente en S3 (metadata de Autoloader) |
# MAGIC | `_source_file_modification_time` | `timestamp` | Fecha de modificación del archivo fuente en S3 |

# COMMAND ----------

from pyspark.sql import functions as F

df_bronze = (
    df_raw
        # Columnas técnicas de auditoría
        .withColumn("_ingested_at",   F.current_timestamp())
        .withColumn("_ingested_date", F.current_date())
        # Metadata de Autoloader: archivo fuente y su fecha de modificación
        .withColumn("_source_file",                    F.col("_metadata.file_path"))
        .withColumn("_source_file_modification_time",  F.col("_metadata.file_modification_time"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escritura en tabla Delta (Bronze)
# MAGIC
# MAGIC - `trigger(availableNow=True)`: procesa todos los archivos pendientes y termina el stream (ideal para jobs batch).
# MAGIC - `checkpointLocation`: garantiza exactamente una vez (exactly-once) y permite reanudar desde donde quedó.
# MAGIC - `mergeSchema`: aplica schema evolution en la tabla Delta destino.

# COMMAND ----------

(
    df_bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(TARGET_TABLE)
)
