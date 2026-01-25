# Databricks notebook source
# MAGIC %md
# MAGIC #Pyspark vs SQL
# MAGIC 🧠 Regla mental que debes repetir en clase
# MAGIC - SQL = declarativo
# MAGIC - PySpark = transformaciones encadenadas sobre DataFrames

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM samples.bakehouse.sales_customers
# MAGIC LIMIT 5

# COMMAND ----------


df = spark.read.table("samples.bakehouse.sales_customers")



df_selected = df.select("*")


df_limited = df_selected.limit(4)


df_limited.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ## DISTINCT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT country
# MAGIC FROM samples.bakehouse.sales_customers

# COMMAND ----------

df_distinct = df_selected.select("country").distinct()

display(df_distinct)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧩 2️⃣ WHERE / FILTER

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM samples.bakehouse.sales_customers
# MAGIC WHERE 1=1
# MAGIC AND country = 'Japan'
# MAGIC AND gender = 'female'
# MAGIC LIMIT 5
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Cell 9
from pyspark.sql.functions import col

df_filtered = df_selected.filter(
    (col("country") == "USA") & 
    (col("gender") == "female")
)

display(df_filtered.limit(4))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧩 5️⃣ ORDER BY + LIMIT

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM samples.bakehouse.sales_customers
# MAGIC ORDER BY first_name ASC
# MAGIC LIMIT 7;

# COMMAND ----------

df_sorted = df.orderBy(col("first_name").asc())

display(df_sorted.limit(4))             

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧩  CREATE COLUMN

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   concat(first_name, " ", last_name) AS full_name,
# MAGIC   concat_ws(', ', city, state, country, continent) AS address,
# MAGIC   35 AS age,
# MAGIC   "random comment" AS comment,
# MAGIC   true AS is_true
# MAGIC FROM samples.bakehouse.sales_customers
# MAGIC
# MAGIC LIMIT 10

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

df_plus_columns = df.select(
    concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
    lit(True).alias("is_true"),
    lit("random comment").alias("comment")
).limit(10)

display(df_plus_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧩  GROUP BY + AGGREGATION

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select country, count(1) as cantidad
# MAGIC from samples.bakehouse.sales_franchises
# MAGIC group by country
# MAGIC order by cantidad desc

# COMMAND ----------

from pyspark.sql.functions import count

df_grouped = (
    df
    .groupBy("country")
    .agg(count("*").alias("count"))
    .orderBy(col("count").desc())
)


display(df_grouped)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧩  JOIN (clave para Silver)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   concat(c.first_name, " ", c.last_name) AS full_name,
# MAGIC   c.country,
# MAGIC   c.gender,
# MAGIC   s.product,
# MAGIC   s.quantity,
# MAGIC   s.paymentMethod,
# MAGIC   s.dateTime
# MAGIC FROM samples.bakehouse.sales_transactions s
# MAGIC JOIN samples.bakehouse.sales_customers c
# MAGIC   ON s.customerID = c.customerID;

# COMMAND ----------

df_sales = spark.read.table("samples.bakehouse.sales_transactions")


df_joined = (df
            .join(df_sales, on="customerID", how="inner")
            .select(
                ("*")
            )
)

df_joined.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   CAST(transactionID AS STRING) AS transactionID,
# MAGIC   CAST(customerID AS STRING) AS customerID,
# MAGIC   -- CAST(franchiseID AS STRING) AS franchiseID,
# MAGIC   CAST(dateTime AS STRING) AS dateTime,
# MAGIC   CAST(product AS STRING) AS product
# MAGIC
# MAGIC FROM samples.bakehouse.sales_transactions;

# COMMAND ----------

df_casted = df_sales.select(
    col("transactionID").cast("bigint").alias("transactionID"),
    col("customerID").cast("string"),
    # col("franchiseID").cast("string"),
    # col("dateTime").cast("string"),
    col("product"),
    col("quantity").cast("bigint"),
    col("unitPrice").cast("double"),

    (col("unitPrice") * col("quantity")).cast("bigint").alias("totalPrice"),
)
    
df_casted.display()


# COMMAND ----------

df_test = df_casted.withColumn("nuevaColumnita", lit("nuevaColumnita_literal!!! "))

df_new = df_test.drop("nuevaColumnita")


df_new.display()

