FROM apache/airflow:2.9.0
USER airflow
RUN pip install apache-airflow-providers-http
RUN pip install dbt-postgres
RUN pip install "protobuf==6.33.6"
RUN pip install "numpy<2" xgboost