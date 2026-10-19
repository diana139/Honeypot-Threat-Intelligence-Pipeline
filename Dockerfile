FROM apache/airflow:3.3.2

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    duckdb==1.5.5 \
    pandas \
    pyarrow \
    apache-airflow-providers-amazon

# dbt in its own venv (in Airflow 3 its dependencies conflict with Airflow's)
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-duckdb==1.11.0 duckdb==1.5.5