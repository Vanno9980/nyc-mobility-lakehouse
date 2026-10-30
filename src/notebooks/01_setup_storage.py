# Databricks notebook source

# COMMAND ----------

# Sostituisci con un catalogo sul quale puoi creare schemi.
catalog = "databricks_dev"

# Schema personale per isolare gli oggetti del progetto.
schema = "vannini_ale"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS
    `{catalog}`.`{schema}`.`landing`
""")

# COMMAND ----------

volume_path = f"/Volumes/{catalog}/{schema}/landing"

print(f"Volume per i file originali: {volume_path}")