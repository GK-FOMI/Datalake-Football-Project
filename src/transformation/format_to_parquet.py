import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, get_json_object, regexp_extract

def format_data_spark(datalake_path):
    spark = SparkSession.builder \
        .appName("FootballAnalytics_Format") \
        .master("local[*]") \
        .getOrCreate()

    yesterday = datetime.now() - timedelta(days=1)
    date_str_raw = yesterday.strftime("%Y%m%d")
    
    # Chemins
    raw_stats_path = os.path.join(datalake_path, f"raw/api_football/players_stats/{date_str_raw}")
    raw_values_path = os.path.join(datalake_path, f"raw/transfermarkt/market_values/{date_str_raw}")
    
    formatted_base_path = os.path.join(datalake_path, f"formatted/{date_str_raw}")
    out_stats_dir = os.path.join(formatted_base_path, "stats_formatted")
    out_values_dir = os.path.join(formatted_base_path, "values_formatted")

    print(f"--- Démarrage Formatage FINAL pour {date_str_raw} ---")

    # --- 1. TRAITEMENT DES STATS (API FOOTBALL) ---
    print(f"Lecture des stats JSON depuis : {raw_stats_path}")
    try:
        df_raw_stats = spark.read.option("multiline", "true").json(raw_stats_path)
    except Exception as e:
        print(f"ERREUR FATALE lecture JSON Stats: {e}")
        sys.exit(1)

    if df_raw_stats.rdd.isEmpty():
        print("ERREUR FATALE : Fichier Stats vide.")
        sys.exit(1)

    if "response" in df_raw_stats.columns:
        print("Structure 'response' détectée. Utilisation de get_json_object.")
        df_exploded = df_raw_stats.select(explode(col("response")).alias("data"))
        
        df_stats_clean = df_exploded.select(
            get_json_object(col("data"), "$.player.id").alias("player_id"),
            get_json_object(col("data"), "$.player.name").alias("player_name_api"),
            get_json_object(col("data"), "$.statistics[0].team.name").alias("team_name"),
            get_json_object(col("data"), "$.statistics[0].league.name").alias("league_name"),
            get_json_object(col("data"), "$.statistics[0].games.appearences").alias("games_played"),
            get_json_object(col("data"), "$.statistics[0].goals.total").alias("goals"),
            lit(date_str_raw).alias("extraction_date")
        )
    elif "player_name" in df_raw_stats.columns:
        df_stats_clean = df_raw_stats.withColumn("extraction_date", lit(date_str_raw))
    else:
        print(f"ERREUR FATALE : Structure JSON inconnue.")
        sys.exit(1)

    df_stats_clean.write.mode("overwrite").parquet(out_stats_dir)
    print(f"✅ Stats sauvegardées : {out_stats_dir}")


    # --- 2. TRAITEMENT DES VALEURS (TRANSFERMARKT) ---
    print(f"Lecture des valeurs JSON depuis : {raw_values_path}")
    try:
        df_raw_values = spark.read.option("multiline", "true").json(raw_values_path)
        
        # --- CORRECTION ICI : Gestion de la colonne manquante ---
        # Si la colonne 'team' n'existe pas dans le JSON, on l'ajoute artificiellement
        if "team" not in df_raw_values.columns:
            print("⚠️ Colonne 'team' absente. Ajout d'une colonne par défaut.")
            df_raw_values = df_raw_values.withColumn("team", lit("Unknown"))
        
        # Maintenant on peut sélectionner 'team' sans risque
        df_values_clean = df_raw_values.select(
            col("player_name"),
            col("team"), 
            col("market_value_raw"),
            lit(date_str_raw).alias("extraction_date")
        )
        
        df_values_clean = df_values_clean.withColumn(
            "market_value_eur", 
            regexp_extract(col("market_value_raw"), r"(\d[\d\.,]*)", 1).cast("float") * 1000000
        )

        df_values_clean.write.mode("overwrite").parquet(out_values_dir)
        print(f"✅ Valeurs sauvegardées : {out_values_dir}")

    except Exception as e:
        print(f"ERREUR FATALE Valeurs : {e}")
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    format_data_spark(sys.argv[1])