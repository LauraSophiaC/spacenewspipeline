from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"owner": "data-eng", "retries": 2}

with DAG(
    dag_id="spacenews_end_to_end",
    start_date=datetime(2026, 2, 18),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["spacenews", "medallion", "dw"],
) as dag:

    # 1) Extract main endpoints (runs inside airflow container)
    extract_main = BashOperator(
        task_id="extract_articles_blogs_reports",
        bash_command=(
            "cd /opt/app && "
            "PYTHONPATH=/opt/app/src RUN_DATE={{ ds }} "
            "python3 -m spacenews.extract.run_extract"
        ),
    )

    # 2) Extract /info (runs inside airflow container)
    extract_info = BashOperator(
        task_id="extract_info",
        bash_command=(
            "cd /opt/app && "
            "PYTHONPATH=/opt/app/src RUN_DATE={{ ds }} "
            "python3 -m spacenews.extract.run_extract_info"
        ),
    )

    # 3) Silver in Spark container
    silver = BashOperator(
        task_id="spark_silver",
        bash_command=(
            "docker exec -i spacenews_spark bash -lc "
            "\"cd /opt/app && PYTHONPATH=/opt/app/src RUN_DATE={{ ds }} "
            "python3 -m spacenews.spark.01_silver_clean\""
        ),
    )

    # 4) Gold in Spark container
    gold = BashOperator(
        task_id="spark_gold",
        bash_command=(
            "docker exec -i spacenews_spark bash -lc "
            "\"cd /opt/app && PYTHONPATH=/opt/app/src "
            "python3 -m spacenews.spark.02_gold_enrich\""
        ),
    )

    # 5) Load DW (runs inside airflow container, connects to spacenews_postgres)
    load_dw = BashOperator(
        task_id="load_dw_postgres",
        bash_command=(
            "cd /opt/app && "
            "PYTHONPATH=/opt/app/src "
            "python3 -m spacenews.dw.load_dw"
        ),
    )

    extract_main >> extract_info >> silver >> gold >> load_dw
