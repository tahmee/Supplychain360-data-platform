# SupplyChain360 — Unified Supply Chain Data Platform

A production-grade data engineering platform that centralises operational supply chain data from multiple sources, enabling real-time analytics across inventory, supplier performance, shipment tracking, and regional sales demand.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Data Sources](#data-sources)
- [Project Workflow](#project-workflow)
- [Data Models](#data-models)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup Guide](#setup-guide)
- [CI/CD Pipeline](#cicd-pipeline)
- [Infrastructure as Code](#infrastructure-as-code)
- [Environment Variables](#environment-variables)

---

## Overview

SupplyChain360 is a fast-growing retail distribution company managing product distribution across hundreds of stores in the United States. Operational data was historically fragmented across warehouse systems, logistics records, supplier logs, and store sales databases — compiled manually into spreadsheets on a weekly cadence, making insights stale before they reached leadership.

This platform solves that by:

- **Centralising** data from S3, PostgreSQL, and Google Sheets into a single cloud data warehouse (Snowflake)
- **Automating** the full ingestion - transformation - quality check pipeline with Apache Airflow
- **Modelling** clean Fact and Dimension tables with dbt for analytics on stockouts, supplier performance, warehouse efficiency, and demand forecasting
- **Enforcing** reproducibility through Docker containerisation, Terraform-managed infrastructure, and GitHub Actions CI/CD

---

## Architecture

---

## Features

| Capability | Details |
|---|---|
| **Multi-source ingestion** | S3 (CSV/JSON), PostgreSQL (partitioned tables), Google Sheets |
| **Idempotent pipelines** | Watermark-based incremental loads; Makes re-run without duplicate free |
| **Raw layer preservation** | All source data stored as Parquet with `_ingestion_timestamp`, `_run_id`, and `_source_*` metadata columns |
| **SCD Type II dimensions** | Full historical tracking of products, suppliers, warehouses, and stores |
| **Incremental fact tables** | `fct_sales` merges on `transaction_id`; only new records are processed each run |
| **Data quality tests** | dbt schema and custom tests on every pipeline run |
| **Failure alerting** | Airflow retry (2× with 1-minute delay) and task-level failure callbacks |
| **Containerised** | Single Docker image bundles Airflow, DAGs, ingestion code, and dbt project |
| **CI/CD** | PRs trigger lint/test/validate; merges to `main` build and push to ECR |
| **Infrastructure as Code** | All AWS and Snowflake resources provisioned via Terraform with remote S3 state |
| **Secrets management** | AWS Secrets Manager in production; `.env`, `aws-vault` and `airflow connections` file for local development |

---

## Data Sources

| Source | Format | Location | Frequency |
|---|---|---|---|
| Product Catalog | CSV | AWS S3 (`supplychain360-data`) | Static (30-day full refresh) |
| Suppliers | CSV | AWS S3 | Static (30-day full refresh) |
| Warehouses | CSV | AWS S3 | Static (30-day full refresh) |
| Inventory Snapshots | CSV | AWS S3 | Daily incremental |
| Shipment Delivery Logs | JSON | AWS S3 | Daily incremental |
| Store Sales Transactions | PostgreSQL tables | Supabase (`sales_YYYY_MM_DD`) | Daily new table |
| Store Locations | — | Google Sheets | Weekly (Mondays) |

---

## Project Workflow

The Airflow DAG `supplychain360_pipeline` runs daily at **02:00 UTC** and executes the following steps:

### Step 1 — Parallel Ingestion

Three ingestion tasks run in parallel:

- **`ingest_s3`** — Reads from the cross-account S3 bucket. Shipments and inventory are loaded incrementally using a watermark on `LastModified`. Products, suppliers, and warehouses do a full refresh every 30 days. All files are converted to Parquet with metadata and uploaded to `supplychain360-bucket-t3`.

- **`ingest_supabase`** — Connects to the Supabase PostgreSQL database, discovers any new `sales_YYYY_MM_DD` tables not yet ingested (tracked via a JSON watermark file), reads them in 50 000-row chunks, converts to Parquet, and uploads to S3.

- **`check_monday` → `ingest_gsheet`** — Checks whether today is Monday before pulling the full store locations dataset from Google Sheets. Uses a Google service account for authentication.

### Step 2 — Load to Snowflake (Parallel COPY INTO)

Seven parallel `SnowflakeOperator` tasks execute `COPY INTO` from the S3 external stage into Snowflake RAW landing tables:

`copy_into_products` · `copy_into_suppliers` · `copy_into_warehouses` · `copy_into_gsheet` · `copy_into_shipments` · `copy_into_inventory` · `copy_into_sales`

### Step 3 — dbt Staging

`dbt_run_staging` builds all staging views (`stg_*`) from the RAW tables. This layer standardises column names, casts types, filters nulls, and corrects inconsistent formats.

### Step 4 — dbt Snapshots

`dbt_snapshot` runs all four SCD Type II snapshots (`snap_products`, `snap_suppliers`, `snap_warehouses`, `snap_stores`), tracking historical changes with `valid_from`, `valid_to`, and `is_current` flags.

### Step 5 — dbt Marts

`dbt_run_marts` builds the Dimension and Fact tables in the `MARTS` schema using the snapshot and staging layers as sources.

### Step 6 — dbt Tests

`dbt_test` runs all schema and data quality tests. A single failing test fails the entire DAG run, preventing bad data from reaching analytics consumers.

---

## Data Models

### Staging Layer (`STAGING` schema)

Views that clean and standardise raw source data. One model per source table.

| Model | Source |
|---|---|
| `stg_products` | RAW products |
| `stg_suppliers` | RAW suppliers |
| `stg_warehouses` | RAW warehouses |
| `stg_stores` | RAW gsheet (Google Sheets) |
| `stg_sales` | RAW sales (dynamic union over `sales_YYYY_MM_DD` tables via macro) |
| `stg_shipments` | RAW shipments |
| `stg_inventory` | RAW inventory |

### Marts Layer (`MARTS` schema)

**Dimensions (SCD Type II — materialized as tables)**

| Model | Grain | Key columns |
|---|---|---|
| `dim_product` | One row per product version | `product_key`, `valid_from`, `valid_to`, `is_current` |
| `dim_supplier` | One row per supplier version | `supplier_key`, `valid_from`, `valid_to`, `is_current` |
| `dim_warehouse` | One row per warehouse version | `warehouse_key`, `valid_from`, `valid_to`, `is_current` |
| `dim_store` | One row per store version | `store_key`, `valid_from`, `valid_to`, `is_current` |
| `dim_date` | One row per calendar date | `date_key`, `year`, `month`, `quarter`, `day_of_week` |

**Facts (incremental — materialized as tables)**

| Model | Grain | Key metrics |
|---|---|---|
| `fct_sales` | One row per product per transaction | quantity sold, revenue, store, product, date |
| `fct_shipments` | One row per shipment | origin warehouse, destination store, delivery status, lead time |
| `fct_inventory` | One row per product per warehouse per day | stock level, days of supply |

---

## Project Structure

```
supplychain360-data-platform/
├── .github/
│   └── workflows/
│       ├── ci.yml                  # PR checks: lint, test, terraform validate
│       └── cd.yml                  # Main branch: build & push Docker image to ECR
│
├── airflow/
│   ├── dags/
│   │   └── pipeline.py             # Main Airflow DAG (supplychain360_pipeline)
│   ├── include/
│   │   ├── config/
│   │   │   └── airflow.cfg         # Airflow configuration
│   │   └── secrets/
│   │       └── profiles.yml        # dbt Snowflake connection profile
│   └── docker-compose.yaml         # Local development stack for Airflow
│
├── infra/
│   ├── bootstrap/                  # One-time S3 backend provisioning
│   │   ├── s3.tf
│   │   ├── locals.tf
│   │   └── variables.tf
│   └── main/                       # Core infrastructure
│       ├── backend.tf              # Remote S3 state
│       ├── providers.tf            # AWS + Snowflake providers
│       ├── s3.tf                   # Landing zone data bucket
│       ├── snowflake.tf            # DB, warehouse, stage, storage integration
│       ├── ecr.tf                  # ECR repository
│       ├── iam.tf                  # IAM roles (Snowflake S3 access, GitHub OIDC)
│       ├── secrets.tf              # AWS Secrets Manager resources
│       ├── locals.tf
│       └── variables.tf
│
├── src/
│   ├── ingestion/
│   │   ├── s3.py                   # S3 ingestion (incremental + full refresh)
│   │   ├── supabase_db.py          # PostgreSQL ingestion (chunked, watermarked)
│   │   └── gsheet.py               # Google Sheets ingestion (full refresh)
│   ├── utils/
│   │   ├── config.py               # Config, logging, Secrets Manager, watermarks
│   │   └── validate.py             # Checksums and column-level validation reports
│   ├── watermark/
│   │   ├── s3.json                 # S3 ingestion state (last run timestamps)
│   │   └── database.json           # Supabase ingestion state (tables ingested)
│   └── tests/                      # Unit tests (pytest)
│
├── supplychain_dbt/
│   ├── models/
│   │   ├── staging/                # Cleaning and standardisation views
│   │   └── marts/                  # Dimension and fact tables
│   ├── snapshots/                  # SCD Type II snapshot definitions
│   ├── macros/                     # Custom macros (dynamic sales union, SCD)
│   ├── tests/                      # Custom dbt generic tests
│   ├── dbt_project.yml
│   └── packages.yml                # dbt_utils dependency
│
├── .env                            # Local environment variables (not committed)
├── Dockerfile                      # Airflow image with src + dbt bundled
├── requirements.txt                # Python dependencies
└── README.md
```

---

## Prerequisites

Ensure the following are installed and configured before setting up the platform:

| Tool | Version | Purpose |
|---|---|---|
| Docker & Docker Compose | Docker ≥ 24 | Run Airflow locally |
| Python | ≥ 3.11 | Local development and testing |
| Terraform | ~1.9 | Provision cloud infrastructure |
| AWS CLI | ≥ 2 | Interact with AWS services |
| Git | — | Version control |

**Cloud accounts required:**

- **AWS** — S3, ECR, IAM, Secrets Manager (Region of choice)
- **Snowflake** —  Account (role: `ACCOUNTADMIN` for initial setup)
- **Google Cloud** — Service account with Google Sheets API enabled

---

## Setup Guide

### 1. Clone the repository

```bash
git clone https://github.com/tahmee/supplychain360-data-platform.git
cd supplychain360-data-platform
```

### 2. Configure environment variables

Create a `.env` file in the project root and populate it with your credentials (see [Environment Variables](#environment-variables) for the full list):

```bash
cp .env.example .env
# Edit .env with your values
```

### 3. Provision infrastructure (Terraform)

**Bootstrap the Terraform state bucket — run once only:**

```bash
cd infra/bootstrap
terraform init
terraform apply
```

**Provision core infrastructure:**

```bash
cd ../main
terraform init
terraform apply
```

This creates:
- S3 bucket (`supplychain360-bucket-t3`) for raw Parquet data
- Snowflake database (`SUPPLYCHAIN360_DB`), warehouse (`SC_WAREHOUSE`), RAW schema, and S3 external stage
- AWS ECR repository for Docker images
- IAM roles for Snowflake S3 access and GitHub Actions OIDC authentication

### 4. Configure the Snowflake external stage

After Terraform applies, retrieve the IAM user ARN that Snowflake uses and add it to the S3 bucket trust policy:

```bash
terraform output snowflake_iam_user_arn
# Add this ARN to the S3 bucket policy in infra/main/s3.tf, then re-apply
```

### 5. Set up the Google Sheets service account

1. Create a project in the Google Cloud Console and enable the **Google Sheets API**
2. Create a Service Account and download the JSON key file
3. For local development, save the key as `src/service_account.json`
4. For production, upload the key to AWS Secrets Manager under `supplychain360/google-credentials`
5. Share your store locations Google Sheet with the service account's email address

### 6. Configure the dbt profile

Edit `airflow/include/secrets/profiles.yml` with your Snowflake connection details:

```yaml
supplychain_dbt:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <SNOWFLAKE_ACCOUNT>
      user: <SNOWFLAKE_USER>
      password: <SNOWFLAKE_PASSWORD>
      role: ACCOUNTADMIN
      database: SUPPLYCHAIN360_DB
      warehouse: SC_WAREHOUSE
      schema: STAGING
      threads: 4
```

### 7. Build the Docker image

```bash
docker build -t supplychain360:local .
```

### 8. Start Airflow locally

```bash
cd airflow

# Run once to initialise the metadata database and create the admin user
docker-compose up airflow-init

# Start all services in the background
docker-compose up -d
```

The Airflow UI is available at **http://localhost:8080**  
Default credentials: `airflow` / `airflow`

### 9. Trigger the pipeline

Activate the `supplychain360_pipeline` DAG from the Airflow UI, or trigger it via CLI:

```bash
docker exec airflow-airflow-scheduler-1 airflow dags trigger supplychain360_pipeline
```

### 10. Run dbt locally (optional)

To develop or test dbt models outside of Airflow:

```bash
cd supplychain_dbt
pip install dbt-snowflake==1.9.3
dbt deps                # Install dbt_utils package
dbt compile             # Validate all models compile without errors
dbt run                 # Execute all models
dbt test                # Run data quality tests
```

---

## CI/CD Pipeline

### Continuous Integration (`ci.yml`)

Triggered on every pull request. Uses path-based change detection so only relevant jobs run:

| Changed path | Job | What it does |
|---|---|---|
| `src/` | `test-ingestion` | Installs dependencies, runs `pytest src/tests/` with coverage upload to Codecov |
| `supplychain_dbt/` | `test-dbt` | Runs `dbt deps` + `dbt compile` (no warehouse connection required) |
| `infra/` | `test-infra` | Runs `terraform validate` + `terraform fmt -check` on `bootstrap/` and `main/` |

### Continuous Deployment (`cd.yml`)

Triggered on every push to `main`. Uses GitHub Actions OIDC.

1. Authenticate to AWS via the OIDC IAM role
2. Log in to Amazon ECR
3. Auto-increment the image version tag (`v1`, `v2`, `v3`, ...)
4. Build and push: `<ECR_REGISTRY>/supplychain360:<TAG>`
5. Update `supplychain360/latest-image` in AWS Secrets Manager with the new image URI

To enable OIDC authentication, set the GitHub repository name in Terraform before applying:

```hcl
# infra/main/variables.tf
github_repo = "your-org/supplychain360-data-platform"
```

---

## Infrastructure as Code

All cloud resources are defined in `infra/` using Terraform. State is stored remotely in `s3://supch360-tfstate-file/terraform.tfstate`.

| Resource | Provider | Description |
|---|---|---|
| `supplychain360-bucket-t3` | AWS S3 | Raw data lake (Parquet files) |
| `supch360-tfstate-file` | AWS S3 | Terraform remote state backend |
| `supplychain360` | AWS ECR | Docker image registry |
| Snowflake OIDC IAM role | AWS IAM | Allows Snowflake to read from S3 |
| GitHub Actions OIDC role | AWS IAM | Allows CI/CD to push to ECR without static keys |
| `SUPPLYCHAIN360_DB` | Snowflake | Data warehouse database |
| `SC_WAREHOUSE` | Snowflake | Compute warehouse (X-SMALL, auto-suspend 20 min) |
| `complete_s3_stage` | Snowflake | External stage pointing to the S3 raw bucket |
| `supplychain360_aws_storage_integration` | Snowflake | S3 storage integration |
| `supplychain360/*` | AWS Secrets Manager | Runtime credentials (production only) |

---

## Environment Variables

| Variable | Description | Environment |
|---|---|---|
| `AIRFLOW_UID` | UID for Airflow process (typically `50000`) | Local |
| `FERNET_KEY` | Airflow Fernet encryption key for connection credentials | Both |
| `_AIRFLOW_WWW_USER_USERNAME` | Airflow admin UI username | Local |
| `_AIRFLOW_WWW_USER_PASSWORD` | Airflow admin UI password | Local |
| `POSTGRES_USER` | Airflow metadata DB username | Local |
| `POSTGRES_PASSWORD` | Airflow metadata DB password | Local |
| `POSTGRES_DB` | Airflow metadata DB name | Local |
| `AWS_DEFAULT_REGION` | AWS region (e.g. `eu-central-1`) | Local |
| `AWS_ACCESS_KEY_ID` | AWS access key — local only; production uses IAM roles | Local |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key — local only | Local |
| `ENV` | Runtime environment flag (`local` or `prod`) | Both |
| `DEPLOY_ENV` | Controls secrets backend (`local` = `.env`, `prod` = Secrets Manager) | Both |
| `DB_CRED` | SQLAlchemy connection string for Supabase PostgreSQL | Both |
| `SHEET_ID` | Google Sheets spreadsheet ID for store locations | Both |
| `DB_SCHEMA` | PostgreSQL schema name (`public`) | Both |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | Both |
| `SNOWFLAKE_USER` | Snowflake username | Both |
| `SNOWFLAKE_PASSWORD` | Snowflake password | Both |
| `SNOWFLAKE_ROLE` | Snowflake role (e.g. `ACCOUNTADMIN`) | Both |
| `SRC_S3_ACCESS_KEY_ID` | Cross-account S3 read credentials — local only | Local |
| `SRC_S3_SECRET_ACCESS_KEY` | Cross-account S3 read credentials — local only | Local |

In production, all secrets are stored in AWS Secrets Manager and loaded at runtime by `src/utils/config.py` when `DEPLOY_ENV=prod`.
