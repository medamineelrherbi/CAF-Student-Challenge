# Databricks notebook source
import time
import random
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType
from datetime import datetime


spark.sql("DROP TABLE IF EXISTS bronze_layer.bronze_crowd_stream")
# 1. Création de la table vide
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_layer.bronze_crowd_stream (
    match_id INT,
    event_time TIMESTAMP,
    tickets_sold_pct DOUBLE,
    crowd_pressure DOUBLE,
    weather_risk DOUBLE
) USING DELTA
""")

print(" Table bronze_crowd_stream (re)créée.")

# 2. Fonction de génération
def generate_data_into_table():
    print(" Début de la génération de flux vers la TABLE...")
    
    # Définition du schéma strict pour que Python ne se trompe pas (Int vs Long)
    schema = StructType([
        StructField("match_id", IntegerType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("tickets_sold_pct", DoubleType(), True),
        StructField("crowd_pressure", DoubleType(), True),
        StructField("weather_risk", DoubleType(), True)
    ])
    
    tickets_state = {}

    for batch in range(1, 6):
        events = []
        
        for _ in range(4):
            match_id = random.randint(1, 50) 
            timestamp = datetime.now()

            if match_id not in tickets_state:
                tickets_state[match_id] = round(random.uniform(0.3, 0.6), 2)
            
            increment = round(random.uniform(0.00, 0.05), 2)
            tickets_state[match_id] = round(min(1.0, tickets_state[match_id] + increment),2)

            # On crée un tuple ou un dictionnaire, c'est plus simple avec un schéma explicite
            events.append((
                match_id,
                timestamp,
                tickets_state[match_id],
                round(random.uniform(0.1, 1.0), 2),
                round(random.uniform(0.0, 10.0), 1)
            ))

        # --- CORRECTION MAJEURE ICI ---
        # On applique le schéma strict lors de la création du DataFrame
        df_new = spark.createDataFrame(events, schema=schema)
        
        # Maintenant l'écriture va passer car match_id est bien un Integer
        df_new.write.format("delta").mode("append").saveAsTable("bronze_layer.bronze_crowd_stream")
        
        print(f" Vague {batch} insérée avec succès.")
        time.sleep(1) 

generate_data_into_table()

# COMMAND ----------

#display(spark.sql("SELECT * FROM bronze_layer.bronze_crowd_stream"))