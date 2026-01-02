# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bronze_layer;
# MAGIC CREATE DATABASE IF NOT EXISTS silver_layer;
# MAGIC CREATE DATABASE IF NOT EXISTS gold_layer;
# MAGIC

# COMMAND ----------

display(spark.sql("SELECT * FROM gold_layer.gold_match_risk"))

# COMMAND ----------

# Commande magique pour débloquer la situation
dbutils.fs.rm("/Volumes/workspace/default/ml_staging/checkpoints/silver", recurse=True)

print("✅ Checkpoint Silver supprimé. Relance ton code Silver maintenant !")

# COMMAND ----------

dbutils.fs.rm("/Volumes/workspace/default/ml_staging/checkpoints/gold", recurse=True)