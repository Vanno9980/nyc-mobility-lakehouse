# Databricks notebook source

# COMMAND ----------

# Verifica l'identità e il contesto della sessione Databricks.
spark.sql("""
    SELECT
        current_user() AS execution_user,
        current_catalog() AS current_catalog,
        current_schema() AS current_schema
""").show(truncate=False)

# COMMAND ----------

# Verifica una semplice elaborazione distribuita.
df = spark.range(1000)

print(f"Numero di record: {df.count()}")