FROM apache/airflow:3.1.3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    pkg-config \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root dbt/ /opt/airflow/dbt/

USER airflow

RUN pip install --upgrade pip setuptools wheel

RUN pip install --no-cache-dir \
    dbt-core \
    dbt-snowflake \
    snowflake-connector-python \
    apache-airflow-providers-snowflake \
    apache-airflow-providers-amazon \
    awswrangler \
    boto3 \
    psycopg2-binary \
    SQLAlchemy \
    apache-airflow-providers-postgres \
    gspread \
    google-auth \
    python-dotenv \
    pandas \
    pyarrow

# dbt deps — uncomment once i add a packages.yml
# RUN cd /opt/airflow/dbt && dbt deps

ENV PATH="/home/airflow/.local/bin:${PATH}"
ENV PYTHONPATH=/opt/airflow/src

ENV DBT_PROFILES_DIR=/opt/airflow/secrets

RUN mkdir -p /opt/airflow/logs && chmod -R 775 /opt/airflow/logs