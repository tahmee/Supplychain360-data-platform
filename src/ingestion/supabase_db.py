import io
import re
import uuid
import gc

import awswrangler as wr
import boto3
import pandas as pd
import pyarrow as pa
from botocore.exceptions import BotoCoreError, ClientError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from utils.config import AWS_REGION, DB_SCHEMA, get_db_cred, get_logger, load_watermark, now_iso, save_watermark
from utils.validate import save_validation_report, validate_parquet


#Config

TABLE_PATTERN = re.compile(r"^sales[_/](\d{4})[_/](\d{2})[_/](\d{2})$")
CHUNK = 50_000
MIN_PART_SIZE = 5 * 1024 * 1024
DEST_BUCKET  = "supplychain360-bucket-t3"
DEST_PREFIX  = "source_data/postgres/"
SOURCE_NAME = "database"

LINES = "-" * 40

logger = get_logger(SOURCE_NAME)

session = boto3.Session(region_name=AWS_REGION)
# Db settings

def build_engine():
    try:
        db_cred = get_db_cred()
        logger.info(f"--- Initializing Database Engine ---")
        engine = create_engine(db_cred, connect_args={"connect_timeout": 10}, pool_pre_ping=True)
        # Verify connectivity immediately
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database engine initialized and verified successfully")
        return engine
    except Exception as e:
        logger.critical(f"Failed: Could not establish database connection: {e}")
        raise 


def list_sales_tables(engine) -> list[str]:
    """Queries the database schema for matching sales tables."""
    query = text("""
        SELECT table_name FROM information_schema.tables
        WHERE  table_schema = :schema
          AND  table_type   = 'BASE TABLE'
          AND  table_name   LIKE 'sales%'
        ORDER  BY table_name ASC
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {"schema": DB_SCHEMA}).fetchall()

    tables = [r[0] for r in rows if TABLE_PATTERN.match(r[0])]
    logger.info("Found %d matching sales table(s).", len(tables))
    return tables

def read_table(engine, table_name: str) -> pd.DataFrame:
    query = f'SELECT * FROM "{DB_SCHEMA}"."{table_name}"'
    chunks = []
    with engine.connect() as conn:
        for chunk in pd.read_sql(query, conn, chunksize=CHUNK):
            chunks.append(chunk)
            gc.collect()

    if not chunks:
        raise ValueError(f"Table '{table_name}' has no rows.")

    df = pd.concat(chunks, ignore_index=True)

    if df.empty:
        raise ValueError(f"Table '{table_name}' has no rows.")
    logger.info(" Read %d rows x %d cols from '%s'", len(df), len(df.columns), table_name)
    return df


# Date Extraction and Bucket Key path

def extract_date_parts(table_name: str) -> tuple[str, str, str]:
    table_match = TABLE_PATTERN.match(table_name)
    if not table_match:
        raise ValueError(f"Cannot parse date from: '{table_name}'")
    return table_match.group(1), table_match.group(2), table_match.group(3)


def build_dest_key(table_name: str) -> str:
    yyyy, mm, dd = extract_date_parts(table_name)
    return f"{DEST_PREFIX}{yyyy}/{mm}/sales_{yyyy}_{mm}_{dd}.parquet"


# Parquet Conversion

def to_parquet(df: pd.DataFrame, table_name: str, ingestion_ts: str, run_id: str, dest_key: str) -> tuple[bytes, pd.DataFrame]:
    """Transforms DataFrame and returns parquet bytes + the source df for validation."""
    if "transaction_id" in df.columns:
        df["transaction_id"] = df["transaction_id"].astype(str)

    source_df = df.copy()

    df["_ingestion_timestamp"] = ingestion_ts
    df["_run_id"] = run_id
    df["_source"] = "supabase_postgres"
    df["_source_table"] = table_name
    df["_ingested_by"] = "supabase_ingest_script"

    s3_path = f"s3://{DEST_BUCKET}/{dest_key}"
    wr.s3.to_parquet(
        df=df,
        path=s3_path,
        boto3_session=session,
        compression="snappy",
        index=False,
    )
    logger.info("Uploaded to %s", s3_path)
    return source_df


# MAIN

def main() -> None:
    run_id = str(uuid.uuid4())
    ingestion_ts = now_iso()

    logger.info(LINES)
    logger.info("SUPABASE INGESTION RUN STARTED")
    logger.info("Run ID    : %s", run_id)
    logger.info("Timestamp : %s", ingestion_ts)
    logger.info(LINES)

    # Initialize Engine
    try:
        engine = build_engine()
    except OperationalError as exc:
        logger.critical("DB connection failed: %s", exc)
        raise SystemExit(1)

    # Discover and Filter Tables
    try:
        all_tables = list_sales_tables(engine)
    except SQLAlchemyError as exc:
        logger.critical("Failed to list tables: %s", exc)
        raise SystemExit(1)

    if not all_tables:
        logger.warning("No sales tables found.")
        return

    watermark = load_watermark(SOURCE_NAME)
    new_tables = [t for t in all_tables if t not in watermark]

    logger.info("Total tables   : %d", len(all_tables))
    logger.info("Already synced : %d", len(watermark))
    logger.info("To ingest      : %d", len(new_tables))

    if not new_tables:
        logger.info("All tables up to date.")
        return
    
    # Processing Loop
    succeeded, failed = [], []

    for table_name in new_tables:
        logger.info("Processing: %s", table_name)
        try:
            dest_key = build_dest_key(table_name)

            df = read_table(engine, table_name)
            source_df = to_parquet(df, table_name, ingestion_ts, run_id, dest_key)
            
            # Free the full df from memory now that it's uploaded
            del df
            gc.collect()

            validation_result = validate_parquet(
                source_df=source_df, 
                s3_client=session.client("s3"),
                bucket=DEST_BUCKET, 
                dest_key=dest_key,
                run_id=run_id, 
                source_name=table_name,
            )
            save_validation_report(validation_result)

            if validation_result["status"] == "FAILED":
                raise ValueError("Validation FAILED. See logs/validate/validation_report.jsonl.")

            watermark[table_name] = ingestion_ts
            succeeded.append(table_name)
            logger.info("Done: %s", table_name)

        except ValueError as exc:
            logger.error("Data/validation error '%s': %s", table_name, exc)
            failed.append({"table": table_name, "error": str(exc)})
        except (BotoCoreError, ClientError) as exc:
            logger.error("S3 error '%s': %s", table_name, exc)
            failed.append({"table": table_name, "error": str(exc)})
        except SQLAlchemyError as exc:
            logger.error("DB error '%s': %s", table_name, exc)
            failed.append({"table": table_name, "error": str(exc)})
        except Exception as exc:
            logger.error("Unexpected error '%s': %s", table_name, exc)
            failed.append({"table": table_name, "error": str(exc)})

    save_watermark(SOURCE_NAME, watermark)

    logger.info(LINES)
    logger.info("RUN SUMMARY  |  Run ID: %s", run_id)
    logger.info("  Succeeded : %d", len(succeeded))
    logger.info("  Failed    : %d", len(failed))
    if failed:
        for f in failed:
            logger.warning("%s — %s", f["table"], f["error"])
    logger.info(LINES)

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()