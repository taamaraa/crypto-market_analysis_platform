from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta

ALERT_EMAIL = "damjan.murgjeski@gmail.com"


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


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_failure,
}

with DAG(
    dag_id='crypto_analytics',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='45 7 * * *',
    catchup=False,
) as dag:

    run_ingestion = BashOperator(
        task_id='run_ingestion',
        bash_command='python /opt/airflow/ingestion/ingest.py',
    )

    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='/home/airflow/.local/bin/dbt run --project-dir /opt/airflow/crypto_analytics --profiles-dir /opt/airflow/crypto_analytics',
    )

    run_dbt_test = BashOperator(
        task_id='run_dbt_test',
        bash_command='/home/airflow/.local/bin/dbt test --project-dir /opt/airflow/crypto_analytics --profiles-dir /opt/airflow/crypto_analytics',
        on_success_callback=notify_success,
    )

    check_market_alerts = BashOperator(
        task_id='check_market_alerts',
        bash_command='python /opt/airflow/ingestion/alerts.py',
    )

    generate_ai_summary = BashOperator(
        task_id='generate_ai_summary',
        bash_command='python /opt/airflow/ingestion/ai_summary.py',
    )

    generate_forecast = BashOperator(
        task_id='generate_forecast',
        bash_command='python /opt/airflow/ingestion/forecast.py',
    )

    score_forecasts = BashOperator(
        task_id='score_forecasts',
        bash_command='python /opt/airflow/ingestion/score_forecasts.py',
    )

    run_ingestion >> run_dbt >> run_dbt_test >> check_market_alerts >> generate_ai_summary >> generate_forecast >> score_forecasts