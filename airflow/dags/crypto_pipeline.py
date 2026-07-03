from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_BIN = "/home/airflow/.local/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt/my_dbt_project"
DBT_PROFILES_DIR = "/home/airflow/.dbt"


def dbt_command(command: str) -> str:
    return f"""
    set -e
    {DBT_BIN} {command} \
    --project-dir {DBT_PROJECT_DIR} \
    --profiles-dir {DBT_PROFILES_DIR}
    """


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
        bash_command=dbt_command("run"),
    )

    run_dbt_test = BashOperator(
        task_id="run_dbt_test",
        bash_command=dbt_command("test"),
    )

    run_ingest >> run_dbt >> run_dbt_test