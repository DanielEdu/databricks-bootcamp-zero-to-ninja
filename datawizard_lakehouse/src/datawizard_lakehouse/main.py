# Databricks notebook source
# Set default catalog and schema
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Using catalog: {catalog}")
print(f"Using schema: {schema}")


# COMMAND ----------

df = spark.table("samples.nyctaxi.trips")
display(df)
