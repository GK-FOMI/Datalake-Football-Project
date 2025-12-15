import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, when, lit

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

        # Normalisation des noms pour la jointure (minuscule + trim)
        df_stats = df_stats.withColumn("join_key", trim(lower(col("player_name_api"))))
        df_values = df_values.withColumn("join_key", trim(lower(col("player_name"))))

        # Jointure [cite: 593]
        df_joined = df_stats.join(df_values, on="join_key", how="left")

        # Calcul du KPI (Index de Valeur Simplifié)
        # Index = (1 / Valeur Marchande) * Facteur Arbitraire (pour l'exemple)
        # Dans un vrai cas, on utiliserait des stats de performance (buts, passes)
        df_final = df_joined.withColumn("value_index", 
            when(col("market_value_eur") > 0, lit(10000000) / col("market_value_eur"))
            .otherwise(0)
        )

        # Sélection finale
        df_output = df_final.select(
            "player_name_api", "team_name", "league_name", 
            "market_value_eur", "value_index", df_stats["extraction_date"]
        )

        out_final = os.path.join(usage_path, "football_analytics_final.parquet")
        df_output.write.mode("overwrite").parquet(out_final)
        print(f"Fichier final généré : {out_final}")

    except Exception as e:
        print(f"Erreur Combinaison Spark: {e}")
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    combine_data_spark(sys.argv[1])