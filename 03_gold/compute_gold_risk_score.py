# Databricks notebook source
from pyspark.sql.functions import col, round, when

# Lire la table Silver
silver = spark.table("silver_layer.silver_match_context")

# Calcul du Risk Score (pondération métier)
gold = silver.withColumn(
    "risk_score",
    round(
        (col("tickets_sold_pct") * 0.3) +           # Poids 30%
        (col("crowd_pressure") * 0.2) +             # Poids 20%
        ((col("weather_risk") / 10) * 0.1) +        # Poids 10% (normalisé)
        ((col("rivalry_score") / 5) * 0.2) +        # Poids 20%
        (col("importance_score") * 0.2),            # Poids 20%
        2
    )
)

# Classification simple
gold = gold.withColumn(
    "risk_level",
    when(col("risk_score") < 0.4, "Low")
    .when(col("risk_score") < 0.7, "Medium")
    .otherwise("High")
)

# Écrire la table Gold
gold.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_layer.gold_match_risk")

print("Gold layer créée !")


# COMMAND ----------

display(spark.sql("SELECT * FROM gold_layer.gold_match_risk"))

# COMMAND ----------

display(gold_table.distinct())