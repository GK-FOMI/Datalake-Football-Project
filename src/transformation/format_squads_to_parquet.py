import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, explode, lower, trim, regexp_extract, regexp_replace, when
from pyspark.sql.types import StructType, StructField, StringType


def format_squads_to_parquet(datalake_path: str, date_str_raw: str):
    spark = SparkSession.builder \
        .appName("FootballAnalytics_Format_Squads") \
        .master("local[*]") \
        .getOrCreate()

    squads_in = os.path.join(datalake_path, f"raw/api_football/players_squads/{date_str_raw}/*.json")
    tm_in = os.path.join(datalake_path, f"raw/transfermarkt/market_values/{date_str_raw}/*.json")
    formatted_path = os.path.join(datalake_path, f"formatted/{date_str_raw}")

    print(f"--- Formatage Squads pour {date_str_raw} ---")
    print(f"SQUADS IN: {squads_in}")
    print(f"TM IN:     {tm_in}")

    # ==============================
    # 1) API Squads -> stats_formatted
    # ==============================
    df = spark.read.option("multiline", "true").json(squads_in)
    df_team = df.select(explode(col("response")).alias("blk"))
    df_players = df_team.select(
        col("blk.team.id").alias("team_id"),
        col("blk.team.name").alias("team_name"),
        explode(col("blk.players")).alias("p")
    )

    df_stats = df_players.select(
        col("p.id").alias("player_id"),
        col("p.name").alias("player_name_api"),
        col("team_name"),
        lit("Ligue 1").alias("league_name"),
        lit(date_str_raw).alias("extraction_date")
    ).dropna(subset=["player_id", "player_name_api", "team_name"])

    out_stats_dir = os.path.join(formatted_path, "stats_formatted")
    df_stats.write.mode("overwrite").parquet(out_stats_dir)
    print(f" ✅ stats_formatted : {out_stats_dir} | n={df_stats.count()}")

    # ==============================
    # 2) Transfermarkt -> values_formatted
    # ==============================
    try:
        values_schema = StructType([
            StructField("player_name", StringType(), True),
            StructField("market_value_raw", StringType(), True),
            StructField("scraped_url", StringType(), True),
        ])

        df_values = spark.read.schema(values_schema).option("multiline", "true").json(tm_in)
        print("Colonnes Transfermarkt lues:", df_values.columns)

        mv_txt = lower(trim(col("market_value_raw")))
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
        print(f" ✅ values_formatted : {out_values_dir} | n={df_clean_values.count()}")

    except Exception as e:
        print(f"⚠️ Erreur Transfermarkt: {e}")
        # Créer un fichier vide pour éviter l'échec du pipeline
        empty_values = spark.createDataFrame([], StructType([
            StructField("player_name", StringType(), True),
            StructField("market_value_raw", StringType(), True),
            StructField("market_value_eur", StringType(), True),
            StructField("extraction_date", StringType(), True),
        ]))
        
        out_values_dir = os.path.join(formatted_path, "values_formatted")
        empty_values.write.mode("overwrite").parquet(out_values_dir)
        print(f" ⚠️ values_formatted (vide) créé : {out_values_dir}")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: format_squads_to_parquet.py <datalake_path> <YYYYMMDD>")
        sys.exit(1)

    format_squads_to_parquet(sys.argv[1], sys.argv[2])
