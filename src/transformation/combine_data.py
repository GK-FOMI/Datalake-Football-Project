import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
# Ajout de 'translate' et 'regexp_replace' pour le nettoyage avancé
from pyspark.sql.functions import col, lower, trim, when, lit, translate, regexp_replace
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

def calculate_just_price_ml(df):
    """
    Utilise Spark MLlib pour prédire la valeur marchande théorique.
    """
    print("--- Démarrage du Module Machine Learning ---")
    
    # 1. Nettoyage pour le ML
    df_clean = df.na.fill(0, subset=["market_value_eur", "player_id"])
    df_clean = df_clean.withColumn("market_value_eur", col("market_value_eur").cast("double")) \
                       .withColumn("player_id", col("player_id").cast("double"))

    # 2. Features
    assembler = VectorAssembler(
        inputCols=["player_id"], 
        outputCol="features",
        handleInvalid="skip"
    )
    
    try:
        training_data = assembler.transform(df_clean)
        
        # 3. Entraînement
        lr = LinearRegression(featuresCol="features", labelCol="market_value_eur")
        model = lr.fit(training_data)
        
        # 4. Prédiction
        predictions = model.transform(training_data)
        
        # 5. Calcul de l'écart
        final_df = predictions.withColumnRenamed("prediction", "fair_price_predicted") \
                              .withColumn("valuation_gap", col("fair_price_predicted") - col("market_value_eur"))
        return final_df
        
    except Exception as e:
        print(f"Attention: Le ML a échoué ({e}), retour aux données sans prédiction.")
        return df.withColumn("fair_price_predicted", lit(0.0)).withColumn("valuation_gap", lit(0.0))


def combine_data_spark(datalake_path):
    spark = SparkSession.builder \
        .appName("FootballAnalytics_Combine") \
        .master("local[*]") \
        .getOrCreate()

    yesterday = datetime.now() - timedelta(days=1)
    date_str_raw = yesterday.strftime("%Y%m%d")
    
    formatted_path = os.path.join(datalake_path, f"formatted/{date_str_raw}")
    usage_path = os.path.join(datalake_path, f"usage/{date_str_raw}")
    
    stats_dir = os.path.join(formatted_path, 'stats_formatted')
    values_dir = os.path.join(formatted_path, 'values_formatted')

    print(f"--- Démarrage Combinaison Spark pour {date_str_raw} ---")

    try:
        df_stats = spark.read.parquet(stats_dir)
        df_values = spark.read.parquet(values_dir)

        # --- AMÉLIORATION DU MATCHING (Nettoyage agressif) ---
        # 1. Définition des accents à supprimer
        # Note: Cette liste couvre les cas courants (français, espagnol, etc.)
        src_chars = "àâäçéèêëîïôöùûüÿñćčžš"
        dst_chars = "aaaceeeeiiouuuyncczs"

        def clean_join_key(column_name):
            # Etape 1: Remplacer les accents
            c = translate(col(column_name), src_chars, dst_chars)
            # Etape 2: Tout en minuscule
            c = lower(c)
            # Etape 3: Garder UNIQUEMENT les lettres (a-z) et chiffres (0-9)
            # On vire les espaces, points, tirets, etc.
            return regexp_replace(c, "[^a-z0-9]", "")

        df_stats = df_stats.withColumn("join_key", clean_join_key("player_name_api"))
        df_values = df_values.withColumn("join_key", clean_join_key("player_name"))

        # Suppression du doublon extraction_date avant la jointure (Correctif précédent)
        df_values = df_values.drop("extraction_date")

        # Jointure
        df_joined = df_stats.join(df_values, on="join_key", how="left")

        # Calcul du KPI simple
        df_with_index = df_joined.withColumn("value_index", 
            when(col("market_value_eur") > 0, lit(10000000) / col("market_value_eur"))
            .otherwise(0)
        )

        # Appel du ML
        df_ml_calculated = calculate_just_price_ml(df_with_index)

        # Sélection finale
        df_output = df_ml_calculated.select(
            col("player_name_api"), 
            col("team_name"), 
            col("league_name"), 
            col("market_value_eur"), 
            col("value_index"), 
            col("fair_price_predicted"),
            col("valuation_gap"),
            col("extraction_date")
        )

        out_final = os.path.join(usage_path, "football_analytics_final.parquet")
        df_output.write.mode("overwrite").parquet(out_final)
        print(f"Fichier final généré avec Matching Avancé : {out_final}")

    except Exception as e:
        print(f"Erreur Combinaison Spark: {e}")
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    combine_data_spark(sys.argv[1])