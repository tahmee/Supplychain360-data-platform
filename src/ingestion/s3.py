import io
import os
import uuid
from datetime import datetime, timezone

import awswrangler as wr
import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError

from utils.config import AWS_REGION, get_logger, load_watermark, now_iso, save_watermark
from utils.validate import save_validation_report, validate_parquet

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

SOURCE_BUCKET   = "supplychain360-data"       # Account-A bucket (read-only)
SOURCE_PREFIX   = "raw/"

DEST_BUCKET     = "supplychain360-bucket-t3"
DEST_PREFIX     = "source_data/s3/"

DYNAMIC_FOLDERS = {"shipments", "inventory"}
STATIC_FOLDERS  = {"products", "warehouses", "suppliers"}

STATIC_REFRESH_DAYS = 30

SERVICE_NAME = "s3"

logger = get_logger(SERVICE_NAME)


# S3 HELPERS
# ---------------------------------------------------------------------------

def list_objects(s3_client, bucket: str, prefix: str) -> list[dict]:
    objects, paginator = [], s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects.extend(page.get("Contents", []))
    return objects


def read_object(s3_client, bucket: str, key: str) -> bytes:
    return s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()

# ---------------------------------------------------------------------------
# WATERMARK HELPERS
# ---------------------------------------------------------------------------

def parse_ts(ts_str: str | None) -> datetime:
    if not ts_str:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    return datetime.fromisoformat(ts_str)

# ---------------------------------------------------------------------------
# PARQUET CONVERSION
# ---------------------------------------------------------------------------

def to_parquet(
    raw_bytes: bytes,
    source_key: str,
    ingestion_ts: str,
    run_id: str,
) -> tuple[bytes, pd.DataFrame]:
    if not raw_bytes:
        raise ValueError(f"File is empty: {source_key}")

    ext = os.path.splitext(source_key)[-1].lower()
    if ext == ".json":
        try:
            df = pd.read_json(io.BytesIO(raw_bytes))
        except ValueError:
            df = pd.read_json(io.BytesIO(raw_bytes), lines=True)
    elif ext in (".csv", ".txt"):
        df = pd.read_csv(io.BytesIO(raw_bytes))
    else:
        raise ValueError(f"Unsupported extension '{ext}': {source_key}")

    if df.empty:
        raise ValueError(f"File has no rows: {source_key}")

    source_df = df.copy()

    df["_ingestion_timestamp"] = ingestion_ts
    df["_run_id"]              = run_id
    df["_source_bucket"]       = SOURCE_BUCKET
    df["_source_key"]          = source_key
    df["_ingested_by"]         = "s3_ingest_script"

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue(), source_df


def build_dest_key(source_key: str) -> str:
    relative = source_key[len(SOURCE_PREFIX):]
    base     = os.path.splitext(relative)[0]
    return f"{DEST_PREFIX}{base}.parquet"

# ---------------------------------------------------------------------------
# CORE INGEST
# ---------------------------------------------------------------------------

def ingest_file(
    src_s3, dst_s3,
    source_key: str,
    ingestion_ts: str,
    run_id: str,
) -> None:
    logger.info("  ↓ reading   s3://%s/%s", SOURCE_BUCKET, source_key)
    raw = read_object(src_s3, SOURCE_BUCKET, source_key)

    parquet_bytes, source_df = to_parquet(raw, source_key, ingestion_ts, run_id)

    dest_key = build_dest_key(source_key)
    logger.info("  ↑ uploading s3://%s/%s", DEST_BUCKET, dest_key)
    dst_s3.put_object(
        Bucket=DEST_BUCKET, Key=dest_key,
        Body=parquet_bytes, ContentType="application/octet-stream",
    )

    vr = validate_parquet(
        source_df=source_df, s3_client=dst_s3,
        bucket=DEST_BUCKET, dest_key=dest_key,
        run_id=run_id, source_name=source_key,
    )
    save_validation_report(vr)
    if vr["status"] == "FAILED":
        raise ValueError(f"Validation FAILED for '{source_key}'.")


def ingest_folder(
    src_s3, dst_s3, folder: str, since: datetime,
    label: str, run_id: str, succeeded: list, failed: list,
) -> None:
    prefix = f"{SOURCE_PREFIX}{folder}/"
    try:
        objects = list_objects(src_s3, SOURCE_BUCKET, prefix)
    except (BotoCoreError, ClientError) as exc:
        logger.error("[%s] Cannot list '%s': %s", label, prefix, exc)
        failed.append({"key": prefix, "error": str(exc)})
        return

    if not objects:
        logger.warning("[%s] No objects under '%s'", label, prefix)
        return

    new_objects = [o for o in objects if o["LastModified"] > since]
    logger.info("[%s] '%s' — %d total | %d new", label, folder, len(objects), len(new_objects))

    for obj in new_objects:
        key = obj["Key"]
        try:
            ingest_file(src_s3, dst_s3, key, now_iso(), run_id)
            logger.info("  ✓ %s", key)
            succeeded.append(key)
        except (BotoCoreError, ClientError) as exc:
            logger.error("  ✗ S3 error   '%s': %s", key, exc)
            failed.append({"key": key, "error": str(exc)})
        except ValueError as exc:
            logger.error("  ✗ Data error '%s': %s", key, exc)
            failed.append({"key": key, "error": str(exc)})
        except Exception as exc:
            logger.error("  ✗ Unexpected '%s': %s", key, exc)
            failed.append({"key": key, "error": str(exc)})

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    run_id          = str(uuid.uuid4())
    ingestion_start = now_iso()

    logger.info("=" * 60)
    logger.info("S3 INGESTION RUN STARTED")
    logger.info("Run ID    : %s", run_id)
    logger.info("Timestamp : %s", ingestion_start)
    logger.info("=" * 60)

    # Both src and dst use ambient credentials (aws-vault locally; task IAM role in prod)
    try:
        src_s3 = boto3.client("s3", region_name=AWS_REGION)
        dst_s3 = boto3.client("s3", region_name=AWS_REGION)
    except Exception as exc:
        logger.critical("AWS session error: %s", exc)
        raise SystemExit(1)

    wm        = load_watermark(SERVICE_NAME)
    succeeded = []
    failed    = []

    # Dynamic – incremental by LastModified
    logger.info("--- DYNAMIC FOLDERS (incremental) ---")
    for folder in DYNAMIC_FOLDERS:
        since = parse_ts(wm.get(f"dynamic.{folder}"))
        logger.info("'%s' | last ingested: %s", folder, since.isoformat())
        ingest_folder(src_s3, dst_s3, folder, since, "DAILY", run_id, succeeded, failed)
        wm[f"dynamic.{folder}"] = ingestion_start

    # Static – full refresh every STATIC_REFRESH_DAYS days
    logger.info("--- STATIC FOLDERS (full refresh every %d days) ---", STATIC_REFRESH_DAYS)
    cutoff = datetime.now(tz=timezone.utc) - pd.Timedelta(days=STATIC_REFRESH_DAYS)
    for folder in STATIC_FOLDERS:
        last_sync = parse_ts(wm.get(f"static.{folder}"))
        if last_sync > cutoff:
            logger.info("Skipping '%s' — synced within %d days", folder, STATIC_REFRESH_DAYS)
            continue
        ingest_folder(src_s3, dst_s3, folder, parse_ts(None), "STATIC", run_id, succeeded, failed)
        wm[f"static.{folder}"] = ingestion_start

    save_watermark(SERVICE_NAME, wm)

    logger.info("=" * 60)
    logger.info("RUN SUMMARY  |  Run ID: %s", run_id)
    logger.info("  Succeeded : %d", len(succeeded))
    logger.info("  Failed    : %d", len(failed))
    if failed:
        for f in failed:
            logger.warning("    ✗ %s — %s", f["key"], f["error"])
    logger.info("=" * 60)

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()