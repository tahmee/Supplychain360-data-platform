import io
import uuid

import boto3
import gspread
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError
from google.oauth2.service_account import Credentials

from utils.config import (
    AWS_REGION,
    get_google_credentials,
    get_logger,
    now_iso,
    GSHEET_ID
)
from utils.validate import save_validation_report, validate_parquet


# Config

SPREADSHEET_ID = GSHEET_ID
SHEET_TAB_NAME = "Sheet1"
FILE_NAME = "store_data"

DEST_BUCKET = "supplychain360-bucket-t3"
DEST_PREFIX = "source_data/gsheet/"

GSHEET_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

logger = get_logger("gsheet")

# ---------------------------------------------------------------------------
# GOOGLE SHEETS READER
# ---------------------------------------------------------------------------

def read_sheet(spreadsheet_id: str, tab_name: str) -> pd.DataFrame:
    """
    Read all records from a Google Sheet tab and return a DataFrame.
    Credential source is determined by DEPLOY_ENV (local file vs Secrets Manager).
    """
    creds_dict = get_google_credentials()
    creds      = Credentials.from_service_account_info(creds_dict, scopes=GSHEET_SCOPES)
    client     = gspread.authorize(creds)

    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
    except gspread.exceptions.SpreadsheetNotFound as exc:
        raise gspread.exceptions.SpreadsheetNotFound(
            f"Spreadsheet not found — check ID and sharing."
        ) from exc

    try:
        worksheet = spreadsheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound as exc:
        raise gspread.exceptions.WorksheetNotFound(
            f"Tab '{tab_name}' not found in spreadsheet."
        ) from exc

    records = worksheet.get_all_records()
    if not records:
        raise ValueError(f"Tab '{tab_name}' is empty.")

    df = pd.DataFrame(records)
    logger.info("Fetched %d rows x %d columns from Google Sheets.", len(df), len(df.columns))
    return df

# Covert Dataframe to Parquet

def to_parquet(df: pd.DataFrame, ingestion_ts: str, run_id: str) -> tuple[bytes, pd.DataFrame]:
    source_df = df.copy()

    df["_ingestion_timestamp"] = ingestion_ts
    df["_run_id"]              = run_id
    df["_source"]              = "google_sheets"
    df["_sheet_tab"]           = SHEET_TAB_NAME
    df["_ingested_by"]         = "gsheets_ingest_script"

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue(), source_df


# Main
lines="-"* 40
def main():
    run_id       = str(uuid.uuid4())
    ingestion_ts = now_iso()

    logger.info(lines)
    logger.info("GOOGLE SHEETS INGESTION STARTED")
    logger.info("Run ID    : %s", run_id)
    logger.info("Timestamp : %s", ingestion_ts)
    logger.info(lines)

    # Read sheet
    try:
        df = read_sheet(SPREADSHEET_ID, SHEET_TAB_NAME)
    except FileNotFoundError as exc:
        logger.critical("%s", exc); raise SystemExit(1)
    except gspread.exceptions.SpreadsheetNotFound as exc:
        logger.critical("%s", exc); raise SystemExit(1)
    except gspread.exceptions.WorksheetNotFound as exc:
        logger.critical("%s", exc); raise SystemExit(1)
    except Exception as exc:
        logger.critical("Sheet read error: %s", exc); raise SystemExit(1)

    # Convert
    try:
        parquet_bytes, source_df = to_parquet(df, ingestion_ts, run_id)
    except Exception as exc:
        logger.error("Parquet conversion failed: %s", exc); raise SystemExit(1)

    # Upload
    dest_key = f"{DEST_PREFIX}{FILE_NAME}.parquet"
    try:
        dest_s3 = boto3.client("s3", region_name=AWS_REGION)
        dest_s3.put_object(
            Bucket=DEST_BUCKET, 
            Key=dest_key,
            Body=parquet_bytes, 
            ContentType="application/octet-stream",
        )
        logger.info("Uploaded -> s3://%s/%s", DEST_BUCKET, dest_key)
    except (BotoCoreError, ClientError) as exc:
        logger.error("S3 upload failed: %s", exc)
        raise SystemExit(1)

    # Validate
    try:
        validate = validate_parquet(source_df=source_df, s3_client=dest_s3, bucket=DEST_BUCKET, dest_key=dest_key, run_id=run_id, source_name=FILE_NAME)
        save_validation_report(validate)
        if validate["status"] == "FAILED":
            logger.error("Validation FAILED. See logs/validate/validation_report.jsonl.")
            raise SystemExit(1)
    except Exception as exc:
        logger.error("Validation process crashed: %s", exc, exc_info=True)
        raise SystemExit(1)
    

    logger.info(lines)
    logger.info("RUN SUMMARY  |  Run ID: %s", run_id)
    logger.info("  Status : SUCCESS")
    logger.info("  Rows   : %d", len(source_df))
    logger.info("  Dest   : s3://%s/%s", DEST_BUCKET, dest_key)
    logger.info(lines)


if __name__ == "__main__":
    main()