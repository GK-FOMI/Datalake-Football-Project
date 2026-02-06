from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DATA_LAKE_PATH = "/opt/airflow/data"
SCRIPTS_PATH = "/opt/airflow/src"

with DAG(
    dag_id="football_analytics_pipeline",
    start_date=datetime(2025, 12, 1),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["big_data", "football", "datalake"],
) as dag:

    extract_stats = BashOperator(
        task_id="1_extract_player_stats",
        bash_command=f"python {SCRIPTS_PATH}/extract_api_football_squads.py {DATA_LAKE_PATH} {{{{ ds_nodash }}}}",
    )

    extract_market_values = BashOperator(
        task_id="2_extract_market_values",
        bash_command=f"python {SCRIPTS_PATH}/ingestion/extract_transfermarkt.py {DATA_LAKE_PATH} {{{{ ds_nodash }}}}",
    )

    format_data = BashOperator(
        task_id="3_format_data_to_parquet",
        bash_command=f"python {SCRIPTS_PATH}/transformation/format_squads_to_parquet.py {DATA_LAKE_PATH} {{{{ ds_nodash }}}}",
    )

    combine_data = BashOperator(
        task_id="4_combine_and_calculate_value_index",
        bash_command=f"python {SCRIPTS_PATH}/transformation/combine_data.py {DATA_LAKE_PATH} {{{{ ds_nodash }}}}",
    )

    index_data = BashOperator(
        task_id="5_index_to_elastic",
        bash_command=f"python {SCRIPTS_PATH}/indexing/index_elastic.py {DATA_LAKE_PATH} http://elasticsearch:9200 {{{{ ds_nodash }}}}",
    )

    [extract_stats, extract_market_values] >> format_data
    format_data >> combine_data
    combine_data >> index_data