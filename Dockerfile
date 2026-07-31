FROM apache/airflow:2.9.3

USER airflow

RUN pip install --no-cache-dir \
    psycopg2-binary \
    requests \
    python-dotenv \
    dbt-core==1.8.8 \
    dbt-postgres==1.8.2

# Forecasting. pandas and numpy already ship with the Airflow image, and ETS
# is written out by hand, so xgboost is the only model that needs installing.
# Pinned below 3.0 and numpy held at the Airflow version: letting pip pull
# numpy 2.x would break the pandas the rest of the image is built against.
RUN pip install --no-cache-dir \
    "xgboost>=2.0,<3.0" \
    "numpy==1.26.4"