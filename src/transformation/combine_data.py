import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, translate, regexp_replace, when, lit, coalesce,
    split, array_sort, concat_ws
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression


def calculate_just_price_ml(df):
    print("--- Démarrage du Module Machine Learning ---")

    df_clean = df.na.fill(0, subset=["market_value_eur", "player_id"])
    df_clean = df_clean.withColumn("market_value_eur", col("market_value_eur").cast("double")) \
                       .withColumn("player_id", col("player_id").cast("double"))

    assembler = VectorAssembler(
        inputCols=["player_id"],
        outputCol="features",
        handleInvalid="skip"
    )

    try:
        training_data = assembler.transform(df_clean)
        lr = LinearRegression(featuresCol="features", labelCol="market_value_eur")
        model = lr.fit(training_data)
        predictions = model.transform(training_data)

        final_df = predictions.withColumnRenamed("prediction", "fair_price_predicted") \
                              .withColumn("valuation_gap", col("fair_price_predicted") - col("market_value_eur"))
        return final_df

    except Exception as e:
        print(f"Attention: Le ML a échoué ({e}), retour aux données sans prédiction.")
        return df.withColumn("fair_price_predicted", lit(0.0)).withColumn("valuation_gap", lit(0.0))


def combine_data_spark(datalake_path: str, date_str_raw: str):
    spark = SparkSession.builder \
        .appName("FootballAnalytics_Combine") \
        .master("local[*]") \
        .getOrCreate()

    formatted_path = os.path.join(datalake_path, f"formatted/{date_str_raw}")
    usage_path = os.path.join(datalake_path, f"usage/{date_str_raw}")

    stats_dir = os.path.join(formatted_path, "stats_formatted")
    values_dir = os.path.join(formatted_path, "values_formatted")

    print(f"--- Démarrage Combinaison Spark pour {date_str_raw} ---")
    print(f"STATS IN:  {stats_dir}")
    print(f"VALUES IN: {values_dir}")

    # Accents -> ASCII
    src_chars = "àâäçéèêëîïôöùûüÿñćčžšÁÀÂÄÇÉÈÊËÎÏÔÖÙÛÜŸÑĆČŽŠ"
    dst_chars = "aaaceeeeiiouuuyncczsAAACEEEEIIOOUUUYNCCZS"

    def norm_keep_spaces(c):
        c = translate(c, src_chars, dst_chars)
        c = lower(trim(c))
        c = regexp_replace(c, r"[^a-z0-9]+", " ")
        c = regexp_replace(c, r"\s+", " ")
        return trim(c)

    def sorted_tokens_key(c):
        cleaned = norm_keep_spaces(c)
        return concat_ws(" ", array_sort(split(cleaned, r"\s+")))

    try:
        df_stats = spark.read.parquet(stats_dir)
        df_values = spark.read.parquet(values_dir)

        # Eviter conflit extraction_date
        if "extraction_date" in df_values.columns:
            df_values = df_values.drop("extraction_date")

        # --- Construire les clés côté API ---
        # IMPORTANT: stats_formatted (squads) n'a PAS player_name_full_api
        api_name = col("player_name_api")

        df_stats = df_stats.withColumn("join_key_direct", norm_keep_spaces(api_name)) \
                           .withColumn("join_key_sorted", sorted_tokens_key(api_name))

        # --- Construire les clés côté Transfermarkt ---
        df_values = df_values.withColumn("join_key_direct", norm_keep_spaces(col("player_name"))) \
                             .withColumn("join_key_sorted", sorted_tokens_key(col("player_name")))

        df_values_slim = df_values.select(
            "join_key_direct", "join_key_sorted",
            "player_name", "market_value_raw", "market_value_eur"
        )

        # 1) JOIN DIRECT
        j1 = df_stats.join(
            df_values_slim.select(
                col("join_key_direct").alias("k"),
                col("player_name").alias("tm_player_name"),
                col("market_value_raw").alias("tm_market_value_raw"),
                col("market_value_eur").alias("tm_market_value_eur")
            ),
            df_stats["join_key_direct"] == col("k"),
            "left"
        ).drop("k")

        # 2) Fallback JOIN SORTED uniquement si pas matché en direct
        not_matched = j1.filter(col("tm_market_value_eur").isNull())

        if not_matched.take(1):
            j2 = not_matched.join(
                df_values_slim.select(
                    col("join_key_sorted").alias("ks"),
                    col("player_name").alias("tm2_player_name"),
                    col("market_value_raw").alias("tm2_market_value_raw"),
                    col("market_value_eur").alias("tm2_market_value_eur")
                ),
                not_matched["join_key_sorted"] == col("ks"),
                "left"
            ).drop("ks")

            fixed = j2.withColumn("tm_player_name", coalesce(col("tm_player_name"), col("tm2_player_name"))) \
                      .withColumn("tm_market_value_raw", coalesce(col("tm_market_value_raw"), col("tm2_market_value_raw"))) \
                      .withColumn("tm_market_value_eur", coalesce(col("tm_market_value_eur"), col("tm2_market_value_eur"))) \
                      .drop("tm2_player_name", "tm2_market_value_raw", "tm2_market_value_eur")

            matched = j1.filter(col("tm_market_value_eur").isNotNull())
            df_joined = matched.unionByName(fixed, allowMissingColumns=True)
        else:
            df_joined = j1

        # market_value_eur final
        df_joined = df_joined.withColumn(
            "market_value_eur",
            coalesce(col("tm_market_value_eur"), lit(0.0)).cast("double")
        )

        # KPI value_index
        df_with_index = df_joined.withColumn(
            "value_index",
            when(col("market_value_eur") > 0, lit(10000000.0) / col("market_value_eur")).otherwise(lit(0.0))
        )

        # ML
        df_ml = calculate_just_price_ml(df_with_index)

        # Output final (pas de player_name_full_api ici)
        df_final = df_ml.select(
            col("player_id"),
            col("player_name_api"),
            col("team_name"),
            col("league_name"),
            col("market_value_eur"),
            col("value_index"),
            col("fair_price_predicted"),
            col("valuation_gap"),
            col("extraction_date"),
            regexp_replace(col("extraction_date"), r"(\d{4})(\d{2})(\d{2})", r"$1-$2-$3").alias("event_time")
        )

        os.makedirs(usage_path, exist_ok=True)
        out = os.path.join(usage_path, "football_analytics_final.parquet")
        df_final.write.mode("overwrite").parquet(out)

        n_total = df_final.count()
        n_matched = df_final.filter(col("market_value_eur") > 0).count()
        print(f" Fichier final généré : {out}")
        print(f" Matched: {n_matched}/{n_total}")

    except Exception as e:
        print(f"Erreur Combinaison Spark: {e}")
        spark.stop()
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: combine_data.py <datalake_path> <YYYYMMDD>")
        sys.exit(1)

    combine_data_spark(sys.argv[1], sys.argv[2])