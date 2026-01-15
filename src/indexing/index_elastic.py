import pandas as pd
import os
import sys
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pyspark.sql import SparkSession 

# Configuration Elasticsearch
ELASTIC_HOSTS = [
    {'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}
]
INDEX_NAME = "football_value_index"

# --- LA FONCTION MANQUANTE À PLACER ICI ---
def create_actions(df, index_name):
    """Génère les documents pour l'indexation bulk."""
    for index, row in df.iterrows():
        doc = row.dropna().to_dict()
        yield {
            "_index": index_name,
            "_id": f"{doc.get('player_name_api')}_{doc.get('extraction_date')}",
            "_source": doc,
        }

def index_data_to_elastic(datalake_path):
    spark = SparkSession.builder \
        .appName("FootballAnalytics_Index") \
        .master("local[*]") \
        .getOrCreate()

    # Date d'hier pour le dossier usage
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    input_dir = os.path.join(datalake_path, f"usage/{yesterday}/football_analytics_final.parquet")

    print(f"--- Démarrage de l'Indexation depuis le dossier: {input_dir} ---")

    try:
        es = Elasticsearch(ELASTIC_HOSTS)
        if not es.ping():
            raise Exception("Connexion Elasticsearch échouée.")
        
        print("Connexion réussie à Elasticsearch. Ping: True")

        # Lecture Spark du dossier
        df_spark = spark.read.parquet(input_dir)
        df_final = df_spark.toPandas()
        print(f"Données chargées : {len(df_final)} lignes à indexer.")

        # Recréation de l'index
        if es.indices.exists(index=INDEX_NAME):
            es.indices.delete(index=INDEX_NAME)
        es.indices.create(index=INDEX_NAME)

        # Indexation Bulk
        successes, errors = bulk(es, create_actions(df_final, INDEX_NAME), raise_on_error=False)
        print(f"Indexation terminée. Succès: {successes}")

    except Exception as e:
        print(f"Erreur FATALE d'Indexation : {e}")
        spark.stop()
        sys.exit(1)
        
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    index_data_to_elastic(sys.argv[1])