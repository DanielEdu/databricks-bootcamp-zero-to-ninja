# Databricks notebook source
# df = spark.read.csv("/Volumes/landing_dev/data-input/data_entry/customers/", header=True, inferSchema=True)


df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ",") \
        .load("/Volumes/landing_dev/data-input/data_entry/customers/")

display(df)

# COMMAND ----------

df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ",") \
        .load("s3://lakehouse-datawizard-dev/landing-dev/customer/")


display(df)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("bronze_dev.external_data.customer_dev")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC DESCRIBE EXTENDED bronze_dev.external_data.customer_dev

# COMMAND ----------

ruta = "s3://lakehouse-datawizard-dev/landing-dev/customer"

display(dbutils.fs.ls(ruta))

# COMMAND ----------


