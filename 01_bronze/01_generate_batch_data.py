# Databricks notebook source
import random
from pyspark.sql import Row
from pyspark.sql.functions import col


# STADIUMS
stadiums = [
    Row(stadium_id=1, stadium="Stade Mohammed V", city="Casablanca", capacity=45000),
    Row(stadium_id=2, stadium="Stade Ibn Batouta", city="Tangier", capacity=65000),
    Row(stadium_id=3, stadium="Prince Moulay Abdellah", city="Rabat", capacity=52000)
]

# Pour les stadiums
df_stadiums = spark.createDataFrame(stadiums)
df_stadiums = df_stadiums.withColumn("stadium_id", col("stadium_id").cast("int"))
df_stadiums.write.format("delta").mode("overwrite").saveAsTable("bronze_layer.bronze_stadiums")

# MATCHES

teams = ["Morocco", "Egypt", "Senegal", "Nigeria", "Ghana", "Algeria", "Tunisia"]
phases = ["Group", "Quarter", "Semi", "Final"]

matches = []
for i in range(1, 51):
    t1, t2 = random.sample(teams, 2)
    matches.append(Row(
        match_id=i,
        team_a=t1,
        team_b=t2,
        phase=random.choice(phases),
        stadium_id=random.randint(1, 3)
    ))

# Pour les matches
df_matches = spark.createDataFrame(matches)
df_matches = df_matches.withColumn("match_id", col("match_id").cast("int")) \
                         .withColumn("stadium_id", col("stadium_id").cast("int"))
df_matches.write.format("delta").mode("overwrite").saveAsTable("bronze_layer.bronze_matches")


# RIVALRIES

rivalries = []
for t1 in teams:
    for t2 in teams:
        if t1 != t2:
            rivalries.append(Row(
                team_a=t1,
                team_b=t2,
                rivalry_score=random.randint(1, 5)
            ))

df_rivalries = spark.createDataFrame(rivalries)
df_rivalries.write.format("delta").mode("overwrite").saveAsTable("bronze_layer.bronze_rivalries")
