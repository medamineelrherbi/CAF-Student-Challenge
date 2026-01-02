# Databricks notebook source
import mlflow
import mlflow.spark
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

# 1. CONFIGURATION MLFLOW
experiment_name = "/Shared/risk_prediction"
mlflow.set_experiment(experiment_name)

# --- CONFIGURATION DU VOLUME UNITY CATALOG ---
# C'est ce chemin qui permet de contourner les erreurs de sécurité
uc_temp_path = "/Volumes/workspace/default/ml_staging"

print(f"Expérience : {experiment_name}")
print(f"Zone de transit UC : {uc_temp_path}")

# 2. CHARGEMENT & PRÉPARATION DES DONNÉES
print(" Chargement des données Gold...")
df_gold = spark.table("gold_layer.gold_match_risk")

feature_cols = ["tickets_sold_pct", "crowd_pressure", "weather_risk", "rivalry_score", "importance_score"]
(trainingData, testData) = df_gold.randomSplit([0.8, 0.2], seed=42)

# 3. DÉFINITION DU PIPELINE
indexer = StringIndexer(inputCol="risk_level", outputCol="label", handleInvalid="keep")
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
num_trees = 20
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=num_trees)

pipeline = Pipeline(stages=[indexer, assembler, rf])

# 4. ENTRAÎNEMENT & TRACKING
print(" Démarrage du Run MLflow...")
from mlflow.models.signature import infer_signature

print(" Démarrage du Run MLflow avec Signature...")

with mlflow.start_run() as run:
    # --- A. Entraînement ---
    model = pipeline.fit(trainingData)
    print(" Modèle entraîné.")

    # --- B. Prédictions & Évaluation ---
    predictions = model.transform(testData)
    accuracy = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy").evaluate(predictions)
    f1_score = MulticlassClassificationEvaluator(labelCol="label", metricName="f1").evaluate(predictions)

    # --- C. LE FIX : Création de la Signature ---
    # 1. On prend un petit exemple des données d'entrée (5 lignes)
    input_example = trainingData.select(feature_cols).limit(5).toPandas()

# 2. On crée la signature uniquement sur ces colonnes
# On précise que l'entrée est uniquement 'feature_cols'
    signature = infer_signature(
        trainingData.select(feature_cols), 
        predictions.select("prediction").limit(5)
    )


    # --- D. Logging des paramètres et métriques ---
    mlflow.log_params({"model_type": "RandomForest", "num_trees": num_trees})
    mlflow.log_metrics({"accuracy": accuracy, "f1_score": f1_score})

    # --- E. SAUVEGARDE DU MODÈLE (Avec Signature et Exemple) ---
    # 3. On logue le modèle avec cette signature propre
    mlflow.spark.log_model(
        spark_model=model,
        artifact_path="risk_model",
        dfs_tmpdir=uc_temp_path,
        signature=signature,
        input_example=input_example
    )
    
    print(f" Modèle enregistré avec Signature dans MLflow !")
    print(f" Run ID : {run.info.run_id}")

# COMMAND ----------

display(spark.sql("SELECT * FROM gold_layer.live_match_predictions"))