# Databricks notebook source
df = spark.read.table("samples.bakehouse.sales_customers")


df.write.mode("overwrite").saveAsTable("bronze_dev.bakehouse.customer_brz")

# COMMAND ----------

df = spark.read.table("samples.bakehouse.sales_franchises")


df.write.mode("overwrite").saveAsTable("bronze_dev.bakehouse.franchises_brz")

# COMMAND ----------

df = spark.read.table("samples.bakehouse.sales_suppliers")


df.write.mode("overwrite").saveAsTable("bronze_dev.bakehouse.suppliers_brz")

# COMMAND ----------

df = spark.read.table("samples.bakehouse.sales_transactions")


df.write.mode("overwrite").saveAsTable("bronze_dev.bakehouse.transactions_brz")
