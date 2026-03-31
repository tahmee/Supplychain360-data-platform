"""
config.py  –  Centralised configuration, logging, and watermark utilities
for SupplyChain360 ingestion scripts.

ENV vars consumed (all optional locally; required in prod via Secrets Manager):
  DB_CRED           – full SQLAlchemy connection URL for Supabase
  GOOGLE_SECRET_ID  – AWS Secrets Manager secret name for GSheet service-account JSON
  AWS_REGION        – defaults to eu-central-1
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------

ROOT_DIR      = Path(__file__).resolve().parents[1]   # src/
LOG_DIR       = ROOT_DIR / "logs"
WATERMARK_DIR = ROOT_DIR / "watermark"

LOG_DIR.mkdir(parents=True, exist_ok=True)
WATERMARK_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# ENVIRONMENT
# ---------------------------------------------------------------------------

AWS_REGION        = os.getenv("AWS_REGION", "eu-central-1")
DB_CRED           = os.getenv("DB_CRED")           # set locally via .env
GOOGLE_SECRET_ID  = os.getenv("GOOGLE_SECRET_ID",  "supplychain360/google-service-account")
DB_SECRET_ID      = os.getenv("DB_SECRET_ID",      "supplychain360/supabase-db-cred")
GSHEET_ID = os.getenv("SHEET_ID")
DB_SCHEMA =os.getenv("DB_SCHEMA")

# Deployment flag: when True, credentials are fetched from Secrets Manager
DEPLOY_ENV = os.getenv("DEPLOY_ENV", "local").lower()   # "local" | "prod"

# ---------------------------------------------------------------------------
# AWS SECRETS MANAGER
# ---------------------------------------------------------------------------

def get_secret(secret_id: str, region: str = AWS_REGION) -> str:
    """Retrieve a plaintext or JSON secret from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region)
    try:
        response = client.get_secret_value(SecretId=secret_id)
        return response.get("SecretString") or response["SecretBinary"].decode()
    except ClientError as exc:
        raise RuntimeError(f"Failed to fetch secret '{secret_id}': {exc}") from exc


def get_google_credentials() -> dict:
    """
    Returns the Google service-account credentials as a dict.
    - Local:  reads SERVICE_ACCOUNT_FILE (path from env or default)
    - Prod:   pulls JSON from AWS Secrets Manager
    """
    if DEPLOY_ENV == "prod":
        raw = get_secret(GOOGLE_SECRET_ID)
        return json.loads(raw)

    sa_file = os.getenv("SERVICE_ACCOUNT_FILE", str(ROOT_DIR / "service_account.json"))
    if not os.path.exists(sa_file):
        raise FileNotFoundError(
            f"Service account file not found: '{sa_file}'. "
            "Set SERVICE_ACCOUNT_FILE env var or switch to DEPLOY_ENV=prod."
        )
    with open(sa_file) as f:
        return json.load(f)


def get_db_cred() -> str:
    """
    Returns the SQLAlchemy DB connection URL.
    - Local:  reads DB_CRED from .env
    - Prod:   pulls from AWS Secrets Manager (stored as a plain URL string)
    """
    if DEPLOY_ENV == "prod":
        return get_secret(DB_SECRET_ID)
    if not DB_CRED:
        raise EnvironmentError(
            "DB_CRED env var not set. Add it to .env or switch to DEPLOY_ENV=prod."
        )
    return DB_CRED

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

def get_logger(service_name: str) -> logging.Logger:
    """
    Returns a named logger that writes to both stdout and a dated log file
    under logs/<service_name>/.  Safe to call multiple times (idempotent).
    """
    service_log_dir = LOG_DIR / service_name
    service_log_dir.mkdir(parents=True, exist_ok=True)

    log_file = service_log_dir / f"ingest_{service_name}_{datetime.now().strftime('%Y-%m-%d')}.log"
    logger   = logging.getLogger(f"sc360.{service_name}")

    if logger.handlers:
        return logger   # already configured

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

# ---------------------------------------------------------------------------
# WATERMARK  (generic, keyed by service)
# ---------------------------------------------------------------------------

def _watermark_path(service_name: str) -> Path:
    return WATERMARK_DIR / f"{service_name}.json"


def load_watermark(service_name: str) -> dict:
    """Load watermark state for the given service (returns {} on first run)."""
    path = _watermark_path(service_name)
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def save_watermark(service_name: str, wm: dict) -> None:
    """Persist watermark state for the given service."""
    path = _watermark_path(service_name)
    with open(path, "w") as f:
        json.dump(wm, f, indent=2)

# ---------------------------------------------------------------------------
# MISC
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()