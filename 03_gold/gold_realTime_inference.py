# Databricks notebook source
import mlflow.spark
import os
from pyspark.sql.functions import col, when

# 1. CONFIGURATION
model_uri = "models:/workspace.default.match_risk_classifier/1"
os.environ['MLFLOW_DFS_TMP'] = "/Volumes/workspace/default/ml_staging"

# 2. CHARGEMENT DU MODÈLE
native_model = mlflow.spark.load_model(model_uri)

# 3. LECTURE ET PRÉPARATION
df_silver_stream = spark.readStream.table("silver_layer.silver_match_context")

# CORRECTION : On inclut les colonnes dont on a besoin pour la table finale
df_prepared = df_silver_stream.select(
    "match_id", "stadium", "event_time", # On garde ces colonnes intactes
    col("tickets_sold_pct").cast("double"),
    col("crowd_pressure").cast("double"),
    col("weather_risk").cast("double"),
    col("rivalry_score").cast("long"),     # Requis par la signature du modèle
    col("importance_score").cast("double")
)

# 4. INFERENCE
# native_model va ajouter les colonnes 'prediction', 'probability', etc. sans toucher aux autres
df_gold_stream = native_model.transform(df_prepared)

# 5. TRANSFORMATION FINALE
# 2. Création du label et nettoyage des colonnes inutiles
df_final = df_gold_stream.withColumn(
    "predicted_risk_level",
    when(col("prediction") == 0.0, "Low")
    .when(col("prediction") == 1.0, "Medium")
    .otherwise("High")
).drop("rawPrediction", "probability", "features") 
# .drop() retire uniquement les colonnes listées, il garde tout le reste (Silver + Risk Level)
dbutils.fs.rm("/Volumes/workspace/default/ml_staging/checkpoints/gold", True)
# 3. Écriture
query = (df_final.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/workspace/default/ml_staging/checkpoints/gold")
    .trigger(availableNow=True)
    .toTable("gold_layer.live_match_predictions"))

query.awaitTermination()
print("Table Gold créée success !")


# COMMAND ----------

#display(spark.sql('SELECT * FROM gold_layer.live_match_predictions'))

# COMMAND ----------

'''  1. Liste des tables à supprimer
tables = [
    "bronze_layer.bronze_matches",
    "bronze_layer.bronze_stadiums",
    "bronze_layer.bronze_rivalries",
    "bronze_layer.bronze_crowd_stream",
    "silver_layer.silver_match_context",
    "gold_layer.live_match_predictions"
]

for table in tables:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Table {table} supprimée.")

checkpoint_path = "/Volumes/workspace/default/ml_staging/checkpoints"
try:
    dbutils.fs.rm(checkpoint_path, recurse=True)
    print("✅ Checkpoints supprimés. Le modèle, lui, est resté intact.")
except:
    print("⚠️ Aucun dossier checkpoint trouvé, rien à supprimer.")
'''

# COMMAND ----------

'''
spark.sql("DROP TABLE IF EXISTS gold_layer.live_match_predictions")

# Suppression du checkpoint Gold
dbutils.fs.rm("/Volumes/workspace/default/ml_staging/checkpoints/gold", recurse=True)
'''