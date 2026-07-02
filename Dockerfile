FROM apache/airflow:2.9.3

USER airflow

RUN pip install --no-cache-dir \
    psycopg2-binary \
    requests \
    dbt-core==1.8.8 \
    dbt-postgres==1.8.2