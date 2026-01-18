import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, explode, lower, trim, translate, regexp_replace,
    regexp_extract, when, concat_ws
)
from pyspark.sql.types import StructType, StructField, StringType


def format_data_to_parquet(datalake_path: str, date_str_raw: str):
    spark = SparkSession.builder \
        .appName("FootballAnalytics_Format") \
        .master("local[*]") \
        .getOrCreate()

    api_path_in = os.path.join(datalake_path, f"raw/api_football/players_stats/{date_str_raw}/*.json")
    scrape_path_in = os.path.join(datalake_path, f"raw/transfermarkt/market_values/{date_str_raw}/*.json")
    formatted_path = os.path.join(datalake_path, f"formatted/{date_str_raw}")

    print(f"--- Démarrage Formatage Spark pour {date_str_raw} ---")
    print(f"API IN: {api_path_in}")
    print(f"SCRAPE IN: {scrape_path_in}")

    # ==========================================
    # 1) API Football (Stats)
    # ==========================================
    try:
        df_api = spark.read.option("multiline", "true").json(api_path_in)

        if "response" not in df_api.columns:
            raise Exception("Champ 'response' introuvable dans le JSON API.")

        df_exploded = df_api.select(explode(col("response")).alias("data"))

        df_stats = df_exploded.select(
            col("data.player.id").alias("player_id"),
            col("data.player.name").alias("player_name_api"),
            col("data.player.firstname").alias("firstname"),
            col("data.player.lastname").alias("lastname"),
            concat_ws(" ", col("data.player.firstname"), col("data.player.lastname")).alias("player_name_full_api"),
            col("data.statistics")[0].getItem("team").getItem("name").alias("team_name"),
            col("data.statistics")[0].getItem("league").getItem("name").alias("league_name"),
            lit(date_str_raw).alias("extraction_date")
        )

        out_stats_dir = os.path.join(formatted_path, "stats_formatted")
        df_stats.write.mode("overwrite").parquet(out_stats_dir)
        print(f"Stats sauvegardées : {out_stats_dir}")

    except Exception as e:
        print(f"Erreur Spark API: {e}")
        spark.stop()
        sys.exit(1)

    # ==========================================
    # 2) Transfermarkt (Valeurs)
    # ==========================================
    try:
        values_schema = StructType([
            StructField("player_name", StringType(), True),
            StructField("market_value_raw", StringType(), True),
            StructField("scraped_url", StringType(), True),
        ])

        df_values = spark.read.schema(values_schema).option("multiline", "true").json(scrape_path_in)

        print("Colonnes Transfermarkt lues:", df_values.columns)

        mv_txt = lower(trim(col("market_value_raw")))

        # Extrait la partie numérique (33,59 / 33.59 / 110,00)
        num_str = regexp_extract(mv_txt, r"([0-9]+(?:[.,][0-9]+)?)", 1)
        num = regexp_replace(num_str, ",", ".").cast("double")

        df_clean_values = df_values.withColumn(
            "market_value_eur",
            when(mv_txt.contains("mio"), num * lit(1000000.0))
            .when(mv_txt.rlike(r"\b[0-9]+(?:[.,][0-9]+)?\s*m\b"), num * lit(1000000.0))
            .when(mv_txt.contains("k"), num * lit(1000.0))
            .when(num.isNotNull(), num)
            .otherwise(lit(0.0))
        ).withColumn("extraction_date", lit(date_str_raw))

        out_values_dir = os.path.join(formatted_path, "values_formatted")
        df_clean_values.write.mode("overwrite").parquet(out_values_dir)
        print(f"Valeurs sauvegardées : {out_values_dir}")

    except Exception as e:
        print(f"Erreur Spark Transfermarkt: {e}")
        spark.stop()
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: format_to_parquet.py <datalake_path> <YYYYMMDD>")
        sys.exit(1)

    format_data_to_parquet(sys.argv[1], sys.argv[2])