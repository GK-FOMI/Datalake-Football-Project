import os
import sys
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pyspark.sql import SparkSession

def index_name_from_date(date_str_raw: str) -> str:
    # YYYYMMDD -> YYYY.MM.DD (index par jour)
    return f"football_value_index-{date_str_raw[0:4]}.{date_str_raw[4:6]}.{date_str_raw[6:8]}"

def ensure_index(es: Elasticsearch, index_name: str):
    # Crée l'index avec mapping si absent (important pour Kibana)
    if es.indices.exists(index=index_name):
        return

    body = {
        "mappings": {
            "properties": {
                "event_time": {"type": "date", "format": "yyyy-MM-dd"},
                "extraction_date": {"type": "keyword"},
                "player_name_api": {"type": "keyword"},
                "team_name": {"type": "keyword"},
                "league_name": {"type": "keyword"},
                "market_value_eur": {"type": "double"},
                "value_index": {"type": "double"},
                "fair_price_predicted": {"type": "double"},
                "valuation_gap": {"type": "double"},
            }
        }
    }
    es.indices.create(index=index_name, body=body)

def create_actions(df_spark, index_name: str):
    # Pas de pandas : on itère sur les lignes Spark
    for row in df_spark.toLocalIterator():
        doc = row.asDict(recursive=True)
        doc_id = f"{doc.get('player_name_api','unknown')}_{doc.get('extraction_date','unknown')}"
        yield {"_index": index_name, "_id": doc_id, "_source": doc}

def main():
    if len(sys.argv) < 4:
        print("Usage: index_elastic.py <datalake_path> <elastic_url> <YYYYMMDD>")
        sys.exit(1)

    datalake_path = sys.argv[1]
    elastic_url = sys.argv[2]
    date_str_raw = sys.argv[3]

    input_dir = os.path.join(datalake_path, f"usage/{date_str_raw}/football_analytics_final.parquet")
    index_name = index_name_from_date(date_str_raw)

    print(f"--- Indexation depuis: {input_dir} ---")
    print(f"--- Elasticsearch: {elastic_url} ---")
    print(f"--- Index cible: {index_name} ---")

    spark = SparkSession.builder.master("local[*]").appName("FootballAnalytics_Index").getOrCreate()

    try:
        es = Elasticsearch(elastic_url)

        if not es.ping():
            raise Exception("Connexion Elasticsearch échouée (ping false).")

        ensure_index(es, index_name)

        df_spark = spark.read.parquet(input_dir)
        total = df_spark.count()
        print(f"Données chargées : {total} lignes à indexer.")

        successes, errors = bulk(
            es,
            create_actions(df_spark, index_name),
            raise_on_error=False,
            request_timeout=120
        )

        print(f"Indexation terminée. Succès: {successes}")
        if errors:
            print(f"Erreurs bulk (extrait): {str(errors)[:400]}")

    except Exception as e:
        print(f"Erreur FATALE d'Indexation : {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()