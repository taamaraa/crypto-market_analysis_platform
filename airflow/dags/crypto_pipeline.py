from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.email import send_email

DBT_BIN = "/home/airflow/.local/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt/crypto_market_analytics"
DBT_PROFILES_DIR = "/home/airflow/.dbt"

ALERT_EMAIL = "tamss9261@gmail.com"


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
    schedule_interval="45 7 * * *",
    catchup=False,
    description="Daily ELT pipeline: ingest raw API data, run dbt models and tests",
) as dag:

    run_ingest = BashOperator(
        task_id="run_ingest",
        bash_command="""
        set -e
        cd /opt/airflow
        python scripts/ingest.py
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

    # Runs after dbt on purpose. It reads binance_metrics, and the accuracy
    # model only ever scores forecasts whose target date already has an
    # actual -- so today's forecast is scored by tomorrow's dbt run and no
    # second dbt invocation is needed.
    run_forecast = BashOperator(
        task_id="run_forecast",
        bash_command="""
        set -e
        cd /opt/airflow
        python scripts/forecast.py
        """,
    )

    run_alerts = BashOperator(
        task_id="run_alerts",
        bash_command="""
        set -e
        cd /opt/airflow
        python scripts/alerts.py
        """,
    )

    # notify_success sits on the last task in the chain, so the success email
    # means the whole pipeline finished -- not just the alert step.
    run_ai_summary = BashOperator(
        task_id="run_ai_summary",
        bash_command="""
        set -e
        cd /opt/airflow
        python scripts/ai_summary.py
        """,
        on_success_callback=notify_success,
    )

    run_ingest >> run_dbt >> run_dbt_test >> run_forecast >> run_alerts >> run_ai_summary