import sys
import os
from elasticsearch import Elasticsearch, helpers
from pyspark.sql import SparkSession
from datetime import datetime

def index_to_elasticsearch(datalake_path: str, es_url: str, date_str: str):
    """
    Indexe les données finales dans Elasticsearch
    """
    spark = SparkSession.builder \
        .appName("FootballAnalytics_Index") \
        .master("local[*]") \
        .getOrCreate()

    input_path = os.path.join(datalake_path, f"usage/{date_str}/football_analytics_final.parquet")
    index_name = f"football_value_index-{date_str}"

    print(f"--- Indexation Elasticsearch pour {date_str} ---")
    print(f"INPUT: {input_path}")
    print(f"ES URL: {es_url}")
    print(f"INDEX: {index_name}")

    try:
        # Charger les données
        df = spark.read.parquet(input_path)
        total_records = df.count()
        print(f"Total records à indexer : {total_records}")

        # Convertir en format ISO 8601 pour Elasticsearch
        # Format : YYYYMMDD -> YYYY-MM-DD
        formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        
        # Convertir en liste de dictionnaires
        records = df.toPandas().to_dict('records')
        
        # Connexion Elasticsearch
        es = Elasticsearch([es_url], request_timeout=60)
        
        if not es.ping():
            raise Exception("Impossible de se connecter à Elasticsearch")
        
        print(f"✅ Connexion Elasticsearch établie")

        # Préparer les actions bulk
        actions = []
        for i, record in enumerate(records):
            # Remplacer event_time par un format ISO
            record['event_time'] = formatted_date
            record['@timestamp'] = formatted_date
            
            action = {
                "_index": index_name,
                "_id": f"{record['player_id']}_{date_str}",
                "_source": record
            }
            actions.append(action)

        # Indexation bulk
        print(f"Indexation de {len(actions)} documents...")
        success, failed = helpers.bulk(es, actions, raise_on_error=False, stats_only=False)
        
        print(f"✅ Indexation terminée")
        print(f"   Succès : {success}")
        print(f"   Échecs : {len(failed) if isinstance(failed, list) else 0}")

        # Vérifier l'index
        count = es.count(index=index_name)
        print(f"   Documents dans l'index : {count['count']}")

        # Créer un alias pour faciliter les requêtes
        alias_name = "football_value_index"
        if es.indices.exists_alias(name=alias_name):
            print(f"Alias '{alias_name}' existe déjà")
        else:
            es.indices.put_alias(index=index_name, name=alias_name)
            print(f"✅ Alias '{alias_name}' créé")

        spark.stop()

    except Exception as e:
        print(f"❌ Erreur indexation Elasticsearch: {e}")
        import traceback
        traceback.print_exc()
        spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: index_elastic.py <datalake_path> <es_url> <YYYYMMDD>")
        sys.exit(1)

    index_to_elasticsearch(sys.argv[1], sys.argv[2], sys.argv[3])
