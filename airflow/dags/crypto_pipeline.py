from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.email import send_email

DBT_BIN = "/home/airflow/.local/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt/my_dbt_project"
DBT_PROFILES_DIR = "/home/airflow/.dbt"

ALERT_EMAIL = "t.srbinoska03@gmail.com"


def notify_success(context):
    subject = "Crypto Pipeline SUCCESS"
    body = """
    Ingest pipeline completed successfully.
    Data was fetched and dbt models/tests finished successfully.
    """
    send_email(to=ALERT_EMAIL, subject=subject, html_content=body)


def notify_failure(context):
    task_id = context.get("task_instance").task_id
    dag_id = context.get("dag").dag_id
    exception = context.get("exception")

    subject = f"Crypto Pipeline FAILED: {task_id}"
    body = f"""
    Pipeline failed.

    DAG: {dag_id}
    Task: {task_id}
    Error: {exception}
    """
    send_email(to=ALERT_EMAIL, subject=subject, html_content=body)


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
    "on_failure_callback": notify_failure,

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
        on_success_callback=notify_success,
    )

    run_ingest >> run_dbt >> run_dbt_test