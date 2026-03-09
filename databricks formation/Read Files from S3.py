# Databricks notebook source
# df = spark.read.csv("/Volumes/landing_dev/data-input/data_entry/customers/", header=True, inferSchema=True)

df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ",") \
        .load("/Volumes/landing_dev/data-input/data_entry/customers/")

display(df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, current_date

df = (spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .load("s3://lakehouse-datawizard-dev/landing-dev/customer/")
)

df_final = df.select(
        df.id.alias("id"),
        df.nombre.alias("name"),
        df.apellido.alias("last_name"),
        df.email.alias("email"),
        df.fecha_nacimiento.alias("birth_date"),
        df.pais.alias("country"),
        df.rol.alias("role")
).withColumn("ingest_timestamp", current_timestamp()) \
.withColumn("ingest_date", current_date())


display(df_final)

# COMMAND ----------

df_final.write.mode("append").saveAsTable("bronze_dev.external_tables.client_brz_ext_tmp")


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze_dev.external_tables.client_brz_ext_tmp

# COMMAND ----------

ruta = "s3://lakehouse-datawizard-dev/bronze-dev/external_tables/client_brz_ext_tmp"

display(dbutils.fs.ls(ruta))

# COMMAND ----------

ruta = "s3://lakehouse-datawizard-dev/bronze-dev/external_tables/client_brz_ext_tmp/_delta_log/"

display(dbutils.fs.ls(ruta))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE bronze_dev.external_tables.client_brz_ext_tmp
# MAGIC ZORDER BY (id)
# MAGIC     

# COMMAND ----------

# DBTITLE 1,Cell 8
# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC VACUUM bronze_dev.external_tables.client_brz_ext_tmp RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE bronze_dev.external_tables.client_brz_ext_tmp 
# MAGIC SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 0 hours');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED bronze_dev.external_tables.client_brz_ext_tmp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_dev.external_tables.client_brz_ext_tmp  as of 4
