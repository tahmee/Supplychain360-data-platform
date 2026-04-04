import hashlib
import io
import json
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError

from .config import AWS_REGION, LOG_DIR, get_logger

log = get_logger("validate")

MONITORING_BUCKET = "supplychain360-bucket-t3"
MONITORING_PREFIX = "monitoring/validation/"

VALIDATION_LOG_FILE = LOG_DIR / "validation_report.jsonl"

# Metadata columns injected during ingestion 
METADATA_COLS = {
    "_ingestion_timestamp",
    "_run_id",
    "_source_bucket",
    "_source_key",
    "_source",
    "_source_table",
    "_source_schema",
    "_spreadsheet_id",
    "_sheet_tab",
    "_ingested_by",
}

# Checksum

def compute_checksum(df):
    data_cols = [c for c in df.columns if c not in METADATA_COLS]
    df_sorted = (
        df[data_cols]
        .sort_values(by=data_cols)
        .reset_index(drop=True)
    )
    csv_bytes = df_sorted.to_csv(index=False).encode("utf-8")
    return hashlib.md5(csv_bytes).hexdigest()


# Column stats

def compute_col_stats(df):
    stats     = {}
    data_cols = [c for c in df.columns if c not in METADATA_COLS]
    for col in data_cols:
        col_stats = {"null_count": int(df[col].isna().sum())}
        if pd.api.types.is_numeric_dtype(df[col]):
            col_stats["min"] = float(df[col].min()) if not df[col].isna().all() else None
            col_stats["max"] = float(df[col].max()) if not df[col].isna().all() else None
            col_stats["sum"] = float(df[col].sum())
        stats[col] = col_stats
    return stats


# Schema comparison

def compare_schemas(source_df, dest_df) :
    source_cols = {c: str(source_df[c].dtype)
                   for c in source_df.columns if c not in METADATA_COLS}
    dest_cols = {c: str(dest_df[c].dtype)
                   for c in dest_df.columns   if c not in METADATA_COLS}

    missing_in_dest = set(source_cols) - set(dest_cols)
    extra_in_dest = set(dest_cols)   - set(source_cols)
    dtype_mismatches = {
        col: {"source": source_cols[col], "dest": dest_cols[col]}
        for col in source_cols
        if col in dest_cols and source_cols[col] != dest_cols[col]
    }
    return {
        "match": not missing_in_dest and not extra_in_dest and not dtype_mismatches,
        "missing_in_dest": list(missing_in_dest),
        "extra_in_dest": list(extra_in_dest),
        "dtype_mismatches": dtype_mismatches,
    }


# Main Validation Function
def validate_parquet(source_df, s3_client, bucket, dest_key, run_id, source_name):
    """
    Read Parquet back from S3 and validate against source DataFrame.
    Returns a result dict; status is 'PASSED' or 'FAILED'.
    """
    log.info("[VALIDATE] Starting validation for '%s'", source_name)

    result = {
        "run_id": run_id,
        "source_name": source_name,
        "dest_key": dest_key,
        "validated_at": datetime.now(tz=timezone.utc).isoformat(),
        "status": "UNKNOWN",
        "checks": {
            "row_count": {"passed": False, "source": 0,  "dest": 0},
            "column_count": {"passed": False, "source": 0,  "dest": 0},
            "schema": {"passed": False, "details": {}},
            "checksum": {"passed": False, "source": "", "dest": ""},
            "col_stats": {"passed": False, "mismatches": []},
        },
        "errors": [],
    }

    # Read back from S3
    try:
        parquet_bytes = s3_client.get_object(Bucket=bucket, Key=dest_key)["Body"].read()
        dest_df = pd.read_parquet(io.BytesIO(parquet_bytes))
        log.info("[VALIDATE] Parquet read back (%d rows)", len(dest_df))
    except Exception as exc:
        msg = f"Could not read Parquet back from S3: {exc}"
        log.error("[VALIDATE] %s", msg)
        result["errors"].append(msg)
        result["status"] = "FAILED"
        return result

    # CHECK 1 – Row count
    src_rows, dest_rows = len(source_df), len(dest_df)
    row_match = src_rows == dest_rows
    result["checks"]["row_count"] = {"passed": row_match, "source": src_rows, "dest": dest_rows}
    log.info("[VALIDATE] %s Row count: src=%d dest=%d", "✓" if row_match else "✗", src_rows, dest_rows)

    # CHECK 2 – Column count
    src_cols  = len([c for c in source_df.columns if c not in METADATA_COLS])
    dest_cols = len([c for c in dest_df.columns   if c not in METADATA_COLS])
    col_match = src_cols == dest_cols
    result["checks"]["column_count"] = {"passed": col_match, "source": src_cols, "dest": dest_cols}
    log.info("[VALIDATE] %s Column count: src=%d dest=%d", "✓" if col_match else "✗", src_cols, dest_cols)

    # CHECK 3 – Schema
    schema_result = compare_schemas(source_df, dest_df)
    result["checks"]["schema"] = {"passed": schema_result["match"], "details": schema_result}
    log.info("[VALIDATE] %s Schema", "✓" if schema_result["match"] else f"✗ {schema_result}")

    # CHECK 4 – Checksum
    try:
        src_cs  = compute_checksum(source_df)
        dest_cs = compute_checksum(dest_df)
        cs_match = src_cs == dest_cs
        result["checks"]["checksum"] = {"passed": cs_match, "source": src_cs, "dest": dest_cs}
        log.info("[VALIDATE] %s Checksum: %s", "✓" if cs_match else "✗", src_cs)
    except Exception as exc:
        msg = f"Checksum failed: {exc}"
        log.error("[VALIDATE] %s", msg)
        result["errors"].append(msg)

    # CHECK 5 – Column stats
    try:
        src_stats  = compute_col_stats(source_df)
        dest_stats = compute_col_stats(dest_df)
        mismatches = []
        for col, s_stat in src_stats.items():
            if col not in dest_stats:
                mismatches.append({"col": col, "issue": "missing in dest"})
                continue
            for metric in ("null_count", "min", "max", "sum"):
                s_val, d_val = s_stat.get(metric), dest_stats[col].get(metric)
                if s_val is not None and d_val is not None and s_val != d_val:
                    mismatches.append({"col": col, "metric": metric, "source": s_val, "dest": d_val})
        stats_ok = len(mismatches) == 0
        result["checks"]["col_stats"] = {"passed": stats_ok, "mismatches": mismatches}
        log.info("[VALIDATE] %s Column stats", "✓" if stats_ok else f"✗ {mismatches}")
    except Exception as exc:
        msg = f"Column stats check failed: {exc}"
        log.error("[VALIDATE] %s", msg)
        result["errors"].append(msg)

    # Overall
    all_passed     = all(v.get("passed", False) for v in result["checks"].values())
    result["status"] = "PASSED" if all_passed and not result["errors"] else "FAILED"
    log.info("[VALIDATE] Overall: %s", result["status"])
    return result


# Persists validation report

def save_validation_report(result) :
    """Append one JSON line per run to the local log and upload to S3 for durability."""
    VALIDATION_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(result) + "\n"

    with open(VALIDATION_LOG_FILE, "a") as f:
        f.write(line)
    log.info("[VALIDATE] Report saved → %s", VALIDATION_LOG_FILE)

    # Upload individual report to S3 so it's available when container restarts
    date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    s3_key = f"{MONITORING_PREFIX}{date_str}/{result['run_id']}_{result['source_name']}.json"
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.put_object(
            Bucket=MONITORING_BUCKET,
            Key=s3_key,
            Body=line.encode(),
            ContentType="application/json",
        )
        log.info("[VALIDATE] Report uploaded → s3://%s/%s", MONITORING_BUCKET, s3_key)
    except (BotoCoreError, ClientError) as exc:
        log.warning("[VALIDATE] S3 upload of report failed (non-fatal): %s", exc)


def load_validation_history():
    if not VALIDATION_LOG_FILE.exists():
        return pd.DataFrame()
    records = []
    with open(VALIDATION_LOG_FILE) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return pd.DataFrame(records)