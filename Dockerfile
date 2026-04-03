FROM apache/airflow:3.1.3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    pkg-config \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --chown=airflow:root requirements.txt .
COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root supplychain_dbt/ /opt/airflow/dbt/

USER airflow

RUN pip install --upgrade pip setuptools wheel

# Pinned application dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Airflow provider extras + dbt (versioned separately from app deps)
RUN pip install --no-cache-dir \
    dbt-core==1.9.4 \
    dbt-snowflake==1.9.3 \
    snowflake-connector-python==3.13.1 \
    apache-airflow-providers-snowflake==6.2.0 \
    apache-airflow-providers-amazon==9.4.0 \
    apache-airflow-providers-postgres==5.13.0

RUN cd /opt/airflow/dbt && dbt deps

ENV PATH="/home/airflow/.local/bin:${PATH}"
ENV PYTHONPATH=/opt/airflow/src

ENV DBT_PROFILES_DIR=/opt/airflow/secrets
ENV DBT_PROJECT_DIR=/opt/airflow/dbt

RUN mkdir -p /opt/airflow/logs && chmod -R 775 /opt/airflow/logs