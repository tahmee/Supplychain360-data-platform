
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# Paths

ROOT_DIR = Path(__file__).resolve().parents[1]   # src/
LOG_DIR = ROOT_DIR / "logs"
WATERMARK_DIR = ROOT_DIR / "watermark"

LOG_DIR.mkdir(parents=True, exist_ok=True)
WATERMARK_DIR.mkdir(parents=True, exist_ok=True)

# Environments

AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")
DB_CRED = os.getenv("DB_CRED")      
GOOGLE_SECRET_ID = os.getenv("GOOGLE_SECRET_ID")
DB_SECRET_ID = os.getenv("DB_SECRET_ID")
GSHEET_ID = os.getenv("SHEET_ID")
DB_SCHEMA =os.getenv("DB_SCHEMA")

# Deployment flag: when True, credentials are fetched from Secrets Manager
DEPLOY_ENV = os.getenv("DEPLOY_ENV").lower()   

# AWS Secrets Manager

def get_secret(secret_id, region= AWS_REGION):
    """Retrieve a plaintext or JSON secret from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region)
    try:
        response = client.get_secret_value(SecretId=secret_id)
        return response.get("SecretString") or response["SecretBinary"].decode()
    except ClientError as exc:
        raise RuntimeError(f"Failed to fetch secret '{secret_id}': {exc}") from exc


def get_google_credentials():
    """
    Returns the Google service-account credentials as a dict.
    Resolution order:
    1. Airflow connection 'google_sheets' (Keyfile JSON field)
    2. AWS Secrets Manager (DEPLOY_ENV=prod)
    3. Local service_account.json file
    """
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection("google_sheets")
        if conn.extra:
            extra = json.loads(conn.extra)
            keyfile = (
                extra.get("keyfile_dict")
                or extra.get("extra__google_cloud_platform__keyfile_dict")
            )
            if keyfile:
                return json.loads(keyfile) if isinstance(keyfile, str) else keyfile
    except Exception as _e:
        logging.getLogger("sc360.config").warning("Airflow connection lookup failed: %s", _e)

    if DEPLOY_ENV == "prod":
        raw = get_secret(GOOGLE_SECRET_ID)
        return json.loads(raw)

    sa_file = os.getenv("SERVICE_ACCOUNT_FILE", str(ROOT_DIR / "service_account.json"))
    if not os.path.exists(sa_file):
        raise FileNotFoundError( f"Service account file not found: '{sa_file}'. "
            "Set SERVICE_ACCOUNT_FILE env var or switch to DEPLOY_ENV=prod."
        )
    with open(sa_file) as f:
        return json.load(f)


def get_db_cred():
    """
    Returns the SQLAlchemy DB connection URL.
    Resolution order:
    1. Airflow connection 'postgres_supabase'
    2. AWS Secrets Manager (DEPLOY_ENV=prod)
    3. DB_CRED env var (.env / local)
    """
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection("postgres_supabase")
        if conn.host and conn.login:
            port = conn.port or 5432
            return (
                f"postgresql+psycopg2://{conn.login}:{conn.password}"
                f"@{conn.host}:{port}/{conn.schema or 'postgres'}"
            )
    except Exception as _e:
        logging.getLogger("sc360.config").warning("Airflow connection lookup failed: %s", _e)

    if DEPLOY_ENV == "prod":
        return get_secret(DB_SECRET_ID)
    if not DB_CRED:
        raise EnvironmentError(
            "DB_CRED env var not set. Add it to .env or switch to DEPLOY_ENV=prod."
        )
    return DB_CRED


def get_dest_s3_session():
    """
    Returns a boto3 Session for the destination (own) S3 bucket.
    Resolution order:
    1. Airflow connection 'aws_default'
    2. Ambient credentials (env vars / IAM role)
    """
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection("aws_default")
        if conn.login and conn.password:
            return boto3.Session(
                aws_access_key_id=conn.login,
                aws_secret_access_key=conn.password,
                region_name=AWS_REGION,
            )
    except Exception as _e:
        logging.getLogger("sc360.config").warning("Airflow connection lookup failed: %s", _e)
    return boto3.Session(region_name=AWS_REGION)


def get_dest_s3_client():
    """
    Returns a boto3 S3 client for the destination (own) bucket.
    Resolution order:
    1. Airflow connection 'aws_default'
    2. Ambient credentials (env vars / IAM role)
    """
    return get_dest_s3_session().client("s3")


def get_src_s3_credentials():
    """
    Returns the S3 static access keys as a dict with keys
    'aws_access_key_id' and 'aws_secret_access_key'.

    Resolution order:
    1. Airflow connection 'aws_src_s3' (preferred when running inside Airflow)
    2. AWS Secrets Manager (when DEPLOY_ENV=prod and no Airflow connection)
    3. SRC_S3_ACCESS_KEY_ID / SRC_S3_SECRET_ACCESS_KEY env vars (local dev)
    """
    # 1. Airflow connection — available when the code runs inside an Airflow task
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection("aws_src_s3")
        if conn.login and conn.password:
            return {
                "aws_access_key_id":     conn.login,
                "aws_secret_access_key": conn.password,
            }
    except Exception as _e:
        logging.getLogger("sc360.config").warning("Airflow connection lookup failed: %s", _e)

    # 2. Secrets Manager (prod deployments without Airflow connection)
    if DEPLOY_ENV == "prod":
        raw = get_secret(os.getenv("SRC_S3_SECRET_ID", "supplychain360/s3-source-keys"))
        return json.loads(raw)

    # 3. Local .env fallback
    key_id = os.getenv("SRC_S3_ACCESS_KEY_ID")
    secret = os.getenv("SRC_S3_SECRET_ACCESS_KEY")
    if not key_id or not secret:
        raise EnvironmentError(
            "No source S3 credentials found. Either create an Airflow connection "
            "'aws_src_s3', set DEPLOY_ENV=prod (Secrets Manager), or set "
            "SRC_S3_ACCESS_KEY_ID and SRC_S3_SECRET_ACCESS_KEY in .env."
        )
    return {"aws_access_key_id": key_id, "aws_secret_access_key": secret}

# Logging setup

def get_logger(service_name):
    """
    Returns a named logger that writes to both stdout and a dated log file
    under logs/<service_name>/.  Safe to call multiple times (idempotent).
    """
    service_log_dir = LOG_DIR / service_name
    service_log_dir.mkdir(parents=True, exist_ok=True)

    log_file = service_log_dir / f"ingest_{service_name}_{datetime.now().strftime('%Y-%m-%d')}.log"
    logger   = logging.getLogger(f"sc360.{service_name}")

    if logger.handlers:
        return logger  

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for handler in (logging.StreamHandler(), logging.FileHandler(log_file, mode="a")):
        handler.setFormatter(fmt)
        logger.addHandler(handler)

    logger.propagate = False
    return logger

# Watermark setup

def _watermark_path(service_name):
    return WATERMARK_DIR / f"{service_name}.json"


def load_watermark(service_name):
    """Load watermark state for the given service (returns {} on first run)."""
    path = _watermark_path(service_name)
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def save_watermark(service_name, wm) :
    """Persist watermark state for the given service."""
    path = _watermark_path(service_name)
    with open(path, "w") as f:
        json.dump(wm, f, indent=2)


def now_iso():
    return datetime.now(tz=timezone.utc).isoformat()