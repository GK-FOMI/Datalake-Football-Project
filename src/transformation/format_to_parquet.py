import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, regexp_replace, explode
# Importations requises pour forcer le schéma
from pyspark.sql.types import StructType, StructField, StringType, FloatType


def format_data_to_parquet(datalake_path):
    # Initialisation de la session Spark (Driver program)
    spark = SparkSession.builder \
        .appName("FootballAnalytics_Format") \
        .master("local[*]") \
        .getOrCreate()

    # Définition des dates
    yesterday = datetime.now() - timedelta(days=1)
    date_str_raw = yesterday.strftime("%Y%m%d")
    
    # Chemins
    api_path_in = os.path.join(datalake_path, f"raw/api_football/players_stats/{date_str_raw}/*.json")
    scrape_path_in = os.path.join(datalake_path, f"raw/transfermarkt/market_values/{date_str_raw}/*.json")
    formatted_path = os.path.join(datalake_path, f"formatted/{date_str_raw}")

    print(f"--- Démarrage Formatage Spark pour {date_str_raw} ---")

    # ==========================================
    # 1. Traitement API Football (Stats)
    # ==========================================
    try:
        # [cite_start]Spark lit directement le JSON et infère le schéma (DataFrames) [cite: 273]
        df_api = spark.read.option("multiline", "true").json(api_path_in)
        
        if "response" in df_api.columns:
            df_exploded = df_api.select(explode(col("response")).alias("data"))
            
            # Sélection des colonnes utiles
            df_stats = df_exploded.select(
                col("data.player.id").alias("player_id"),
                col("data.player.name").alias("player_name_api"),
                col("data.statistics")[0].getItem("team").getItem("name").alias("team_name"),
                col("data.statistics")[0].getItem("league").getItem("name").alias("league_name"),
                lit(date_str_raw).alias("extraction_date")
            )
            
            # [cite_start]Sauvegarde en Parquet (Action) [cite: 471]
            out_stats_dir = os.path.join(formatted_path, "stats_formatted")
            df_stats.write.mode("overwrite").parquet(out_stats_dir)
            print(f"Stats sauvegardées : {out_stats_dir}")
            
    except Exception as e:
        print(f"Erreur Spark API: {e}")
        # L'échec ici est considéré non fatal pour le moment, mais on pourrait y ajouter sys.exit(1)
        # pour s'assurer que si l'API est HS, le DAG échoue.

    # ==========================================
    # 2. Traitement Scraping (Valeurs)
    # ==========================================
    try:
        # --- NOUVEAU: Schéma forcé pour garantir la colonne market_value_raw ---
        values_schema = StructType([
            StructField("player_name", StringType(), True),
            StructField("market_value_raw", StringType(), True),
            StructField("scraped_url", StringType(), True),
        ])
        
        # Lecture du JSON brut avec le schéma forcé
        df_values = spark.read.schema(values_schema).json(scrape_path_in)
        
        # Nettoyage de la valeur marchande :
        # [cite_start]1. Gestion du format "mio. €" (Votre JSON) [cite: 89]
        # 2. Gestion de "M €" ou "k €" (Cas général)
        
        # Remplacement de la virgule par un point, retrait des caractères non numériques/alpha
        df_clean_values = df_values.withColumn("market_value_eur", 
            regexp_replace(col("market_value_raw"), ",", ".")
        )

        df_clean_values = df_clean_values.withColumn("market_value_eur", 
            when(col("market_value_eur").contains("mio."), 
                 regexp_replace(col("market_value_eur"), "[^0-9.]", "").cast("float") * 1000000)
            .when(col("market_value_eur").contains("M"), 
                 regexp_replace(col("market_value_eur"), "[^0-9.]", "").cast("float") * 1000000)
            .when(col("market_value_eur").contains("k"), 
                 regexp_replace(col("market_value_eur"), "[^0-9.]", "").cast("float") * 1000)
            .otherwise(0.0)
        ).withColumn("extraction_date", lit(date_str_raw))
        
        # Sauvegarde dans un DOSSIER (comme corrigé précédemment)
        out_values_dir = os.path.join(formatted_path, "values_formatted")
        df_clean_values.write.mode("overwrite").parquet(out_values_dir)
        print(f"Valeurs sauvegardées : {out_values_dir}")

    except Exception as e:
        print(f"Erreur Spark Scraping FATALE: {e}")
        # --- NOUVEAU : FORCER L'ÉCHEC POUR NE PAS BLOQUER LA TÂCHE 4 ---
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    format_data_to_parquet(sys.argv[1])