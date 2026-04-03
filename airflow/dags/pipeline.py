from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator

log = logging.getLogger(__name__)

# Snowflake and dbt config

SNOWFLAKE_CONN_ID = "snowflake_default"
DB                = "SUPPLYCHAIN360_DB"
SCHEMA            = "RAW"
STAGE             = f'{DB}.{SCHEMA}."complete_s3_stage"'
FILE_FORMAT       = f"FORMAT_NAME = '{DB}.{SCHEMA}.PARQUET_FORMAT'"
COPY_OPTS         = "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE  ON_ERROR = CONTINUE"

DBT_DIR          = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/secrets"
DBT_TARGET       = os.getenv("DBT_TARGET", "prod")
DBT_GLOBAL       = "dbt --no-use-colors"
DBT_FLAGS        = f"--project-dir {DBT_DIR} --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}"


# Failure callback

def _on_failure(context: dict) -> None:
    ti = context["task_instance"]
    log.error(
        "PIPELINE FAILURE | Task: %s | Execution: %s | Log: %s",
        ti.task_id,
        context.get("logical_date") or context.get("data_interval_start"),
        ti.log_url,
    )

# Default args


default_args = {
    "params": {
        "dag_owner": "Tammy",
    },
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure":    False,
    "on_failure_callback": _on_failure,
}


# Ingestion callables


def _run_s3_ingestion(**_):
    from ingestion.s3 import main
    main()


def _run_supabase_ingestion(**_):
    from ingestion.supabase_db import main
    main()


def _run_gsheet_ingestion(**_):
    from ingestion.gsheet import main
    main()


def _is_monday(**_) -> bool:
    """ShortCircuitOperator: only run gsheet on Mondays."""
    return True 



# Snowflake COPY INTO SQL commands


def _copy_sql(table, stage_path):
    return [
        f"""
        CREATE TABLE IF NOT EXISTS {DB}.{SCHEMA}.{table}
        USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION => '@{STAGE}/{stage_path}',
                    FILE_FORMAT => '{DB}.{SCHEMA}.PARQUET_FORMAT'
                )
            )
        );
        """,
        f"""
        COPY INTO {DB}.{SCHEMA}.{table}
        FROM @{STAGE}/{stage_path}
        FILE_FORMAT = ({FILE_FORMAT})
        {COPY_OPTS};
        """,
    ]


COPY_STATEMENTS = {
    "products": _copy_sql("PRODUCTS", "s3/products/"),
    "suppliers": _copy_sql("SUPPLIERS", "s3/suppliers/"),
    "warehouses": _copy_sql("WAREHOUSES", "s3/warehouses/"),
    "gsheet": _copy_sql("GSHEET", "gsheet/"),
    "shipments": _copy_sql("SHIPMENTS", "s3/shipments/"),
    "inventory": _copy_sql("INVENTORY", "s3/inventory/"),
    "sales": _copy_sql("SALES", "postgres/"),
}


# DAG


with DAG(
    dag_id="supplychain360_pipeline",
    description="SupplyChain360 end-to-end pipeline: ingest - COPY INTO -dbt",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["supplychain360"],
) as dag:

    # Start
    start = EmptyOperator(task_id="start")

    # task 1: Ingestion (parallel)
    ingest_s3 = PythonOperator(
        task_id="ingest_s3",
        python_callable=_run_s3_ingestion
    )

    ingest_supabase = PythonOperator(
        task_id="ingest_supabase",
        python_callable=_run_supabase_ingestion
    )

    check_monday = ShortCircuitOperator(
        task_id="check_monday",
        python_callable=_is_monday
    )

    ingest_gsheet = PythonOperator(
        task_id="ingest_gsheet",
        python_callable=_run_gsheet_ingestion
    )

    all_ingested = EmptyOperator(
        task_id="all_ingested",
        trigger_rule="all_done"
    )

    # task 2: COPY INTO (parallel) 
    copy_tasks = [
        SQLExecuteQueryOperator(
            task_id=f"copy_into_{source}",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=sql,
            doc_md=f"Load new {source} Parquet files from S3 into RAW.{source.upper()}."
        )
        for source, sql in COPY_STATEMENTS.items()
    ]

    all_loaded = EmptyOperator(task_id="all_loaded")

    # task 3: dbt staging 
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_GLOBAL} run --select staging {DBT_FLAGS}"
    )

    # task 4: dbt snapshot
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_GLOBAL} snapshot {DBT_FLAGS}"
    )

    # task 5: dbt run marts
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT_GLOBAL} run --select marts+ {DBT_FLAGS}"
    )

    # task 6: dbt test
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_GLOBAL} test --select staging+ marts+ {DBT_FLAGS}"
    )

    end = EmptyOperator(task_id="end")

    #  Dependencies
    start >> [ingest_s3, ingest_supabase, check_monday]
    check_monday >> ingest_gsheet
    [ingest_s3, ingest_supabase, ingest_gsheet] >> all_ingested
    all_ingested >> copy_tasks >> all_loaded
    all_loaded >> dbt_run_staging >> dbt_snapshot >> dbt_run_marts >> dbt_test >> end
