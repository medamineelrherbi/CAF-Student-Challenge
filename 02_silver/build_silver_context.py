# Databricks notebook source
from pyspark.sql.functions import coalesce, lit, when, col

# 1. Lecture des tables (on s'assure que c'est bien du INT)
stadiums = spark.table("bronze_layer.bronze_stadiums").select(
    col("stadium_id").cast("int"), "stadium", "city", "capacity"
)

matches = spark.table("bronze_layer.bronze_matches").select(
    col("match_id").cast("int"), "team_a", "team_b", "phase", 
    col("stadium_id").cast("int")
)

rivalries = spark.table("bronze_layer.bronze_rivalries") # Déjà String/Int, c'est bon

# 2. Lecture du Stream (Déjà INT d'après ton schéma)
df_bronze_stream = spark.readStream.table("bronze_layer.bronze_crowd_stream")

# 3. Jointure corrigée
silver_stream = (
    df_bronze_stream.alias("stream")
    .join(matches.alias("m"), "match_id", "left")
    .join(stadiums.alias("s"), "stadium_id", "left")
    .join(rivalries.alias("r"), 
          (col("m.team_a") == col("r.team_a")) & (col("m.team_b") == col("r.team_b")), 
          "left")
    .select(
        col("m.stadium_id"), # Préfixe m. obligatoire
        col("stream.match_id"), 
        col("stream.event_time"), 
        col("stream.tickets_sold_pct"), 
        col("stream.crowd_pressure"), 
        col("stream.weather_risk"),
        col("m.team_a"),      # Résout l'ambiguïté [m.team_a vs r.team_a]
        col("m.team_b"),      # Résout l'ambiguïté [m.team_b vs r.team_b]
        col("m.phase"),
        col("s.stadium"),
        col("s.city"),
        col("s.capacity"),
        col("r.rivalry_score") # On prend le score de la table rivalry
    )
    .withColumn("rivalry_score", coalesce(col("rivalry_score"), lit(1)))
    .withColumn("importance_score", 
                when(col("phase") == "Final", 1.0)
                .when(col("phase") == "Semi", 0.8)
                .when(col("phase") == "Quarter", 0.6)
                .otherwise(0.4))
)


dbutils.fs.rm("/Volumes/workspace/default/ml_staging/checkpoints/silver", True)

query = (silver_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/workspace/default/ml_staging/checkpoints/silver")
    .trigger(availableNow=True) 
    .toTable("silver_layer.silver_match_context"))

query.awaitTermination()

# COMMAND ----------

#display(spark.table("silver_layer.silver_match_context"))