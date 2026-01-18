import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, explode


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
    # 1) API Squads -> stats_formatted (version squads)
    # ==============================
    df = spark.read.option("multiline", "true").json(squads_in)

    # structure: response: [ { team: {...}, players: [...] }, ... ]
    df_team = df.select(explode(col("response")).alias("blk"))
    df_players = df_team.select(
        col("blk.team.id").alias("team_id"),
        col("blk.team.name").alias("team_name"),
        explode(col("blk.players")).alias("p")
    )

    df_stats = df_players.select(
        col("p.id").alias("player_id"),
        col("p.name").alias("player_name_api"),  # ici c'est déjà le nom "complet/standard"
        col("team_name"),
        lit("Ligue 1").alias("league_name"),
        lit(date_str_raw).alias("extraction_date")
    ).dropna(subset=["player_id", "player_name_api", "team_name"])

    out_stats_dir = os.path.join(formatted_path, "stats_formatted")  # on écrase le même dossier
    df_stats.write.mode("overwrite").parquet(out_stats_dir)
    print(f" stats_formatted (squads) : {out_stats_dir} | n={df_stats.count()}")

    # ==============================
    # 2) Transfermarkt -> values_formatted (on réutilise ton parsing)
    # ==============================
    # 👉 on réutilise TON script existant pour TM, donc ici on ne fait rien.
    # Assure-toi juste d'avoir déjà lancé format_to_parquet.py pour TM.

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: format_squads_to_parquet.py <datalake_path> <YYYYMMDD>")
        sys.exit(1)

    format_squads_to_parquet(sys.argv[1], sys.argv[2])
