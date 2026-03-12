# Databricks notebook source
from pyspark.sql.functions import current_timestamp, date_format

# COMMAND ----------

dbutils.widgets.text("source_table", "samples.bakehouse.sales_customers")
dbutils.widgets.text("target_table", "bronze_dev.bakehouse.customer_brz")

# COMMAND ----------

source = dbutils.widgets.get("source_table") 
target = dbutils.widgets.get("target_table")


# COMMAND ----------

df = spark.table(source)

# COMMAND ----------

# DBTITLE 1,Cell 2


df_final = df.select(
    df.customerID.cast("String").alias("customerID"),
    df.first_name.cast("String").alias("first_name"),
    df.last_name.cast("String").alias("last_name"),
    df.email_address.cast("String").alias("email_address"),
    df.phone_number.cast("String").alias("phone_number"),
    df.address.cast("String").alias("address"),
    df.city.cast("String").alias("city"),
    df.state.cast("String").alias("state"),
    df.country.cast("String").alias("country"),
    df.continent.cast("String").alias("continent"),
    df.postal_zip_code.cast("String").alias("postal_zip_code"),
    df.gender.cast("String").alias("gender"),
    current_timestamp().alias("ingest_timestamp"),
    current_timestamp().cast("date").alias("ingest_date"),
)



# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable(target)

# COMMAND ----------


