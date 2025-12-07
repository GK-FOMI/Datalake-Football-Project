from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator 
from datetime import datetime, timedelta
import os 

# --- Configuration des chemins ---
# Le chemin DATA_LAKE_PATH doit correspondre au volume monté dans docker-compose
DATA_LAKE_PATH = "/opt/airflow/data"
SCRIPTS_PATH = "/opt/airflow/src"

with DAG(
    dag_id='football_analytics_pipeline',
    # Le DAG démarre à cette date et est schedulé pour s'exécuter après cette date
    start_date=datetime(2025, 12, 1),
    # Exécution quotidienne pour la Source 1 (obligatoire pour le cahier des charges)
    schedule_interval=timedelta(days=1), 
    catchup=False,
    tags=['big_data', 'football', 'datalake']
) as dag:

    # ----------------------------------------------------------------------
    # ÉTAPE 1 : INGESTION (Extraction)
    # ----------------------------------------------------------------------
    
    # Tâche 1: Extraction des Statistiques (Source 1 - API)
    extract_stats = BashOperator(
        task_id='1_extract_player_stats',
        # Lance le script Python qui est dans src/ingestion
        bash_command=f'python {SCRIPTS_PATH}/ingestion/extract_api_football.py {DATA_LAKE_PATH}'
    )

    # Tâche 2: Extraction de la Valeur Marchande (Source 2 - Scraping)
    extract_market_values = BashOperator(
        task_id='2_extract_market_values',
        bash_command=f'python {SCRIPTS_PATH}/ingestion/extract_transfermarkt.py {DATA_LAKE_PATH}'
    )

    # ----------------------------------------------------------------------
    # ÉTAPE 3 : FORMATAGE & COMBINAISON (Transformation)
    # ----------------------------------------------------------------------
    
    # Tâche 3: Formatage (Parquet, Normalisation)
    format_data = BashOperator(
        task_id='3_format_data_to_parquet',
        bash_command=f'python {SCRIPTS_PATH}/transformation/format_to_parquet.py {DATA_LAKE_PATH}'
    )

    # Tâche 4: Combinaison (Calcul de l'Index de Valeur, ML)
    combine_data = BashOperator(
        task_id='4_combine_and_calculate_value_index',
        bash_command=f'python {SCRIPTS_PATH}/transformation/combine_data.py {DATA_LAKE_PATH}'
    )

    # ----------------------------------------------------------------------
    # ÉTAPE 4 : INDEXATION (Exposition)
    # ----------------------------------------------------------------------
    
    # Tâche 5: Indexation vers Elasticsearch
    index_data = BashOperator(
        task_id='5_index_to_elastic',
        # Nous passons l'URL interne du service Elastic
        bash_command=f'python {SCRIPTS_PATH}/indexing/index_elastic.py {DATA_LAKE_PATH} http://elasticsearch:9200'
    )

    # Définition des dépendances
    [extract_stats, extract_market_values] >> format_data
    format_data >> combine_data
    combine_data >> index_data