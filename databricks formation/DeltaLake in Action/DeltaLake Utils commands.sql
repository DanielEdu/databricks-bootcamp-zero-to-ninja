-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### DESCRIBE TABLE
-- MAGIC Muestra el schema básico: columnas, tipo de dato y nullabilidad.
-- MAGIC Es equivalente a ver la estructura lógica de la tabla.
-- MAGIC Útil para revisar rápidamente campos antes de hacer un SELECT.

-- COMMAND ----------

describe bronze_dev.bakehouse.customer_brz

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DESCRIBE EXTENDED
-- MAGIC Incluye el schema y metadata adicional.
-- MAGIC Muestra propiedades de tabla y comentarios.
-- MAGIC Más detallado que DESCRIBE TABLE.

-- COMMAND ----------

DESCRIBE EXTENDED bronze_dev.bakehouse.customer_brz

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###DESCRIBE DETAIL
-- MAGIC Devuelve información técnica avanzada en formato tabular.
-- MAGIC Incluye ubicación física, tamaño en bytes, número de archivos, formato (delta), etc.
-- MAGIC Muy útil para análisis operativo y optimización.

-- COMMAND ----------

DESCRIBE DETAIL bronze_dev.bakehouse.customer_brz

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DESCRIBE HISTORY
-- MAGIC Muestra el historial de versiones (time travel).
-- MAGIC Incluye operación (WRITE, MERGE, DELETE), usuario y timestamp.
-- MAGIC Clave para auditoría y debugging en pipelines.

-- COMMAND ----------

DESCRIBE HISTORY bronze_dev.bakehouse.customer_brz

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###DESCRIBE FORMATTED
-- MAGIC Versión aún más detallada y organizada en secciones.
-- MAGIC Incluye info de almacenamiento, provider, propiedades y estadísticas.
-- MAGIC Se usa cuando necesitas diagnóstico completo.

-- COMMAND ----------

DESCRIBE FORMATTED bronze_dev.bakehouse_ext.customer_brz_ext

-- COMMAND ----------

SHOW CREATE TABLE bronze_dev.bakehouse.customer_brz

-- COMMAND ----------


