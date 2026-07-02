from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    description="Daily ELT pipeline: ingest raw API data, run dbt models and tests",
) as dag:

    run_ingest = BashOperator(
        task_id="run_ingest",
        bash_command="""
        set -e
        cd /opt/airflow
        python ingest.py
        """,
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="""
        set -e
        /home/airflow/.local/bin/dbt run \
        --project-dir /opt/airflow/dbt/my_dbt_project \
        --profiles-dir /home/airflow/.dbt
        """,
    )

    run_dbt_test = BashOperator(
        task_id="run_dbt_test",
        bash_command="""
        set -e
        /home/airflow/.local/bin/dbt test \
        --project-dir /opt/airflow/dbt/my_dbt_project \
        --profiles-dir /home/airflow/.dbt
        """,
    )

    run_ingest >> run_dbt >> run_dbt_test