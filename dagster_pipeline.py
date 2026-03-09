"""
Dagster pipeline for scheduling Python scripts.

Replaces watchdog_script.py with a proper Dagster-native orchestration setup.

Jobs and their schedules:
  - risk_raw_data_job   -> daily at 03:30
  - risk_engine_job     -> daily at 04:30
  - mkt_com_dump_job    -> daily at 08:30
  - mkt_summary_job     -> daily at 09:30

To run the dashboard:
    dagster dev -f dagster_pipeline.py -p 8081
Then open: http://localhost:8081
"""

import subprocess
import sys
from pathlib import Path

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    In,
    Nothing,
    OpExecutionContext,
    ScheduleDefinition,
    job,
    op,
    define_asset_job,
)

from scripts.CT_events import (
    ct_watermarks,
    ct_lookups,
    fe_clevertap,
    fe_clevertap_indexes,
)

from scripts.allsku import (
    allsku_extract,
    allsku_load,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

VENV_PYTHON = r"C:\\Python_scripts\\.venv\\Scripts\\python.exe"
# FIXED: Changed from scripts subfolder to root folder where your scripts actually are
SCRIPTS_DIR = Path(r"C:\\Python_scripts\\scripts\\")


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _run_script(context: OpExecutionContext, script_name: str) -> None:
    """Run a script inside the project venv and stream its output to Dagster logs."""
    script_path = SCRIPTS_DIR / script_name
    context.log.info(f"Running script: {script_path}")

    # Verify script exists before running
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    result = subprocess.run(
        [VENV_PYTHON, str(script_path)],
        capture_output=True,
        text=True,
        cwd=str(SCRIPTS_DIR),  # Set working directory to script location
    )

    if result.stdout:
        context.log.info(result.stdout)
    if result.stderr:
        context.log.warning(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"Script {script_name} failed with exit code {result.returncode}\n"
            f"STDERR: {result.stderr}"
        )

    context.log.info(f"Script {script_name} completed successfully.")


# ---------------------------------------------------------------------------
# Ops  — each op has a detailed description and tags for the dashboard
# ---------------------------------------------------------------------------

@op(
    description=(
        "📥 Risk Raw Data ETL — Extracts and loads raw order+risk data.\n\n"
        "**What it does:**\n"
        "1. Connects to the **source PostgreSQL database** (DB1)\n"
        "2. Fetches multiple datasets via SQL:\n"
        "   - `order_master` — orders + order items joined with shipping/financials\n"
        "   - `category_tree` — 6-level product category hierarchy\n"
        "   - `return_master` — return requests with QC and rider info\n"
        "   - `return_qty` — aggregated return quantities per order+variant\n"
        "   - `fwd_pkg` — forward logistics package codes\n"
        "   - `free_shipping` — free shipping campaign flags\n"
        "3. Merges all datasets on OrderId, CustomerOrderCode, VariantId, etc.\n"
        "4. Calculates derived columns:\n"
        "   - `ReturnQty` (clipped to order quantity)\n"
        "   - `EligibleForRefund` (based on payment method, cancel/return status)\n"
        "   - `CustomerShipping`, `CartupShipping`, `SellerShipping` (weight-proportional)\n"
        "5. Filters out internal sellers (SellerId 1,3) and packaging materials\n"
        "6. Drops and recreates `public.daily_order_report` in the **target DB** (DB2)\n"
        "7. Uploads the final dataset in 10k-row chunks with progress logging\n\n"
        "**Source script:** `risk_raw_data.py` (765 lines)\n"
        "**Target table:** `public.daily_order_report`\n"
        "**Databases:** DB1 (source) → DB2 (target)\n"
        "**Typical runtime:** Several minutes (depends on order volume)"
    ),
    tags={
        "domain": "risk",
        "type": "etl",
        "source": "postgresql",
        "target": "postgresql",
        "script": "risk_raw_data.py",
        "target_table": "public.daily_order_report",
    },
)
def risk_raw_data_op(context: OpExecutionContext):
    """Run risk_raw_data.py — full ETL for daily order report."""
    _run_script(context, "risk_raw_data.py")


@op(
    description=(
        "⚙️ Risk Engine — Fraud & abuse detection on daily order data.\n\n"
        "**What it does:**\n"
        "1. Reads `public.daily_order_report` from DB2 (output of risk_raw_data)\n"
        "2. Cleans data: drops null PaymentMethod, filters packaging materials,\n"
        "   removes test orders, fixes Receiver Phone (Bangla→English digits)\n"
        "3. Maps `VoucherId` → `VoucherCode` via Google Sheets voucher master\n"
        "4. Aggregates at **Device ID** and **Receiver Phone** level:\n"
        "   - Number of accounts, GOS (gross order sessions), GIS (gross item sessions)\n"
        "   - GMV, voucher subsidy, shipping subsidy\n"
        "   - Voucher codes used and their counts\n"
        "   - Free shipping IDs used and their counts\n"
        "5. **Voucher Abuse Detection:**\n"
        "   - Checks if entities (5+ accounts) exceeded per-user voucher limits\n"
        "   - Flags APG/APH multi-use and WINNER voucher multi-use\n"
        "   - Calculates VoucherSubsidyLost\n"
        "6. **Free Shipping Abuse Detection:**\n"
        "   - Checks free shipping campaign usage vs per-user limits from Google Sheet\n"
        "   - Calculates ShippingSubsidyLost\n"
        "7. **Cancellation & Return Analysis:**\n"
        "   - Computes return rates, valid/invalid returns, GMV at risk\n"
        "   - Extracts phone numbers embedded in addresses\n"
        "8. Builds final risk score and uploads to DB2\n\n"
        "**Source script:** `scripts/risk_engine.py` (1954 lines)\n"
        "**Input table:** `public.daily_order_report`\n"
        "**External sources:** Google Sheets (voucher master, free shipping limits)\n"
        "**Depends on:** risk_raw_data_op (must run first)"
    ),
    tags={
        "domain": "risk",
        "type": "fraud-detection",
        "source": "postgresql + google-sheets",
        "target": "postgresql",
        "script": "risk_engine.py",
        "depends_on": "risk_raw_data",
    },
    ins={"after_risk_raw_data": In(Nothing)},
)
def risk_engine_op(context: OpExecutionContext):
    """Run risk_engine.py — fraud & abuse detection pipeline."""
    _run_script(context, "risk_engine.py")


@op(
    description=(
        "📊 Marketing COM Dump — Extracts commerce data and uploads to Google Sheets.\n\n"
        "**What it does:**\n"
        "1. Loads SQL query from `query/mkt_com_dump.sql`\n"
        "2. Executes the query against the **source DB** (DB1) via SQLAlchemy\n"
        "3. Authenticates with Google Sheets using `service_account.json`\n"
        "4. Clears the target Google Sheet and uploads the fresh data\n\n"
        "**Source script:** `mkt_com_dump.py` (217 lines)\n"
        "**SQL query:** `query/mkt_com_dump.sql`\n"
        "**Target:** Google Sheets → spreadsheet `1YYNZXrQ...` → sheet `COM_Dump`\n"
        "**Auth:** Service account (`service_account.json`)\n"
        "**Typical runtime:** 1–3 minutes"
    ),
    tags={
        "domain": "marketing",
        "type": "etl",
        "source": "postgresql",
        "target": "google-sheets",
        "script": "mkt_com_dump.py",
        "sheet": "COM_Dump",
    },
)
def mkt_com_dump_op(context: OpExecutionContext):
    """Run mkt_com_dump.py — commerce data dump to Google Sheets."""
    _run_script(context, "mkt_com_dump.py")


@op(
    description=(
        "📈 Marketing Summary — Fetches, transforms, and loads marketing KPIs.\n\n"
        "**What it does:**\n"
        "1. Fetches marketing data from Google Sheets (sheet: `Overall (Adj)`)\n"
        "2. Renames duplicate columns to avoid conflicts\n"
        "3. Transforms and standardizes:\n"
        "   - Cleans currency columns (removes ৳, $, commas → numeric)\n"
        "   - Converts percentage columns (CTR, CR, CIR, etc.)\n"
        "   - Renames ~50+ columns to standardized snake_case format\n"
        "   - Handles date parsing and edge cases\n"
        "4. KPI columns include:\n"
        "   - **Spend:** taka, USD, acquisition, retargeting, branding\n"
        "   - **Traffic:** impressions, clicks, CPC, CTR\n"
        "   - **App:** installs (organic/paid), CPI, DAU, MAU\n"
        "   - **Conversion:** add-to-cart, checkout, purchase funnel\n"
        "   - **Business:** buyers (gross/net), orders, revenue, AOV, ROAS\n"
        "   - **Attribution:** organic %, paid %, Google %, Facebook %, CRM %\n"
        "5. Truncates and replaces `daily_mkt_data_summary` in **target DB** (DB2)\n"
        "6. Verifies row count after upload for data integrity\n\n"
        "**Source script:** `mkt_summary.py` (346 lines)\n"
        "**Input:** Google Sheets → spreadsheet `1YYNZXrQ...` → sheet `Overall (Adj)`\n"
        "**Target table:** `daily_mkt_data_summary` (DB2)\n"
        "**Typical runtime:** 1–2 minutes"
    ),
    tags={
        "domain": "marketing",
        "type": "etl",
        "source": "google-sheets",
        "target": "postgresql",
        "script": "mkt_summary.py",
        "target_table": "daily_mkt_data_summary",
    },
)
def mkt_summary_op(context: OpExecutionContext):
    """Run mkt_summary.py — marketing summary ETL to database."""
    _run_script(context, "mkt_summary.py")


# CT Events pipeline is now defined as native Dagster Assets imported above
# All SKU Catalog pipeline is now defined as native Dagster Assets imported above


# ---------------------------------------------------------------------------
# Jobs — full descriptions with context on what each pipeline achieves
# ---------------------------------------------------------------------------

@job(
    description=(
        "🔴 Risk Raw Data Pipeline\n\n"
        "Runs the full ETL that builds `public.daily_order_report` in DB2.\n\n"
        "**Schedule:** Daily at 03:30 (Asia/Dhaka)\n"
        "**Duration:** Several minutes\n"
        "**Downstream:** `risk_engine_job` depends on this completing first.\n\n"
        "This job extracts order data, returns, categories, shipping info from DB1, "
        "merges everything, calculates shipping cost splits and refund eligibility, "
        "then writes the consolidated report to DB2."
    ),
    tags={
        "domain": "risk",
        "schedule": "03:30 daily",
        "priority": "high",
    },
)
def risk_raw_data_job():
    risk_raw_data_op()


@job(
    description=(
        "🟠 Risk Engine Pipeline\n\n"
        "Runs fraud and abuse detection on the daily order report.\n\n"
        "**Schedule:** Daily at 04:30 (Asia/Dhaka) — 1 hour after risk_raw_data\n"
        "**Depends on:** `risk_raw_data_job` must complete first\n"
        "**Duration:** 5–15 minutes\n\n"
        "This job reads `daily_order_report`, aggregates by Device ID and Phone number, "
        "detects voucher abuse (APG/APH/WINNER multi-use, per-user limit violations) "
        "and free shipping abuse. Outputs risk scores, subsidy losses, and abuse type flags.\n\n"
        "**External dependencies:** Google Sheets for voucher master and free shipping limits."
    ),
    tags={
        "domain": "risk",
        "schedule": "04:30 daily",
        "priority": "high",
        "depends_on": "risk_raw_data_job",
    },
)
def risk_engine_job():
    risk_engine_op(after_risk_raw_data=risk_raw_data_op())


@job(
    description=(
        "🔵 Marketing COM Dump Pipeline\n\n"
        "Extracts commerce data from DB1 and uploads it to Google Sheets.\n\n"
        "**Schedule:** Daily at 08:30 (Asia/Dhaka)\n"
        "**Duration:** 1–3 minutes\n\n"
        "Runs a SQL query from `query/mkt_com_dump.sql`, clears the target sheet, "
        "and uploads the results to the `COM_Dump` tab. The marketing team uses this "
        "sheet as their primary commerce data source."
    ),
    tags={
        "domain": "marketing",
        "schedule": "08:30 daily",
        "priority": "medium",
    },
)
def mkt_com_dump_job():
    mkt_com_dump_op()


@job(
    description=(
        "🟢 Marketing Summary Pipeline\n\n"
        "Fetches marketing KPIs from Google Sheets and loads into DB2.\n\n"
        "**Schedule:** Daily at 09:30 (Asia/Dhaka) — 1 hour after mkt_com_dump\n"
        "**Duration:** 1–2 minutes\n\n"
        "Reads the `Overall (Adj)` sheet, transforms ~50+ KPI columns "
        "(spend, traffic, app installs, DAU, conversions, revenue, attribution), "
        "standardizes naming to snake_case, and replaces `daily_mkt_data_summary` in DB2.\n"
        "Includes row-count verification after upload."
    ),
    tags={
        "domain": "marketing",
        "schedule": "09:30 daily",
        "priority": "medium",
        "depends_on": "mkt_com_dump_job",
    },
)
def mkt_summary_job():
    mkt_summary_op()


allsku_job = define_asset_job(
    name="allsku_job",
    selection="*allsku_load",
    description=(
        "🟣 All SKU Catalog Transfer (Native Dagster)\n\n"
        "Bulk-copies the product catalog (mv_allsku) from the report service DB \n"
        "to the local analytics DB.\n\n"
        "**Schedule:** Fridays at 10:00 (Asia/Dhaka)\n"
        "**Duration:** Varies by catalog size\n\n"
        "Transfers 10 key columns (Vertical, categories, seller info, product IDs) \n"
        "using Pandas `to_sql` bulk inserts. Includes clear metadata logging \n"
        "and post-transfer row count verification."
    ),
    tags={
        "domain": "catalog",
        "schedule": "06:00 daily",
        "priority": "medium",
    },
)


ct_events_job = define_asset_job(
    name="ct_events_job",
    selection="*fe_clevertap_indexes",  # selects all assets upstream of the final index step
    description=(
        "🔵 CleverTap Events Pipeline (Native Dagster)\n\n"
        "Incremental ETL that loads user event data from Trino into PostgreSQL.\n\n"
        "**Schedule:** Daily at 06:00 (Asia/Dhaka)\n"
        "**Duration:** Varies by event volume; uses 1-hour windowed pagination\n\n"
        "Fetches new events from `user_event_log_jan_2026_main_5` since the last watermark, "
        "enriches them with product/category data from `mv_allsku`, and appends to "
        "`public.fe_clevertap`. On first run, loads the last 30 days of history."
    ),
    tags={
        "domain": "clevertap",
        "schedule": "06:00 daily",
        "priority": "medium",
    },
)

# ---------------------------------------------------------------------------
# Schedules  — with descriptions explaining the timing rationale
# ---------------------------------------------------------------------------

risk_raw_data_schedule = ScheduleDefinition(
    job=risk_raw_data_job,
    cron_schedule="30 3 * * *",   # 03:30 daily
    name="risk_raw_data_schedule",
    execution_timezone="Asia/Dhaka",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "⏰ Runs risk_raw_data_job daily at 03:30 AM (Asia/Dhaka).\n"
        "Scheduled early morning so the daily_order_report is ready "
        "before the Risk Engine runs at 04:30."
    ),
)

risk_engine_schedule = ScheduleDefinition(
    job=risk_engine_job,
    cron_schedule="30 4 * * *",   # 04:30 daily
    name="risk_engine_schedule",
    execution_timezone="Asia/Dhaka",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "⏰ Runs risk_engine_job daily at 04:30 AM (Asia/Dhaka).\n"
        "Starts 1 hour after risk_raw_data to ensure the daily_order_report "
        "table is fully populated before fraud detection begins."
    ),
)

mkt_com_dump_schedule = ScheduleDefinition(
    job=mkt_com_dump_job,
    cron_schedule="30 8 * * *",   # 08:30 daily
    name="mkt_com_dump_schedule",
    execution_timezone="Asia/Dhaka",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "⏰ Runs mkt_com_dump_job daily at 08:30 AM (Asia/Dhaka).\n"
        "Scheduled before business hours so the marketing team's "
        "Google Sheet has fresh commerce data by start of day."
    ),
)

mkt_summary_schedule = ScheduleDefinition(
    job=mkt_summary_job,
    cron_schedule="30 9 * * *",   # 09:30 daily
    name="mkt_summary_schedule",
    execution_timezone="Asia/Dhaka",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "⏰ Runs mkt_summary_job daily at 09:30 AM (Asia/Dhaka).\n"
        "Runs 1 hour after mkt_com_dump so that the Google Sheet data "
        "is freshly updated before being pulled into the database."
    ),
)

allsku_schedule = ScheduleDefinition(
    job=allsku_job,
    cron_schedule="0 10 * * 5",    # 10:00 AM on Fridays (5)
    name="allsku_schedule",
    execution_timezone="Asia/Dhaka",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "⏰ Runs allsku_job every Friday at 10:00 AM (Asia/Dhaka).\n"
        "Scheduled after risk pipelines complete and before business hours "
        "so the product catalog is fresh for the marketing team."
    ),
)

ct_events_schedule = ScheduleDefinition(
    job=ct_events_job,
    cron_schedule="0 6 * * *",     # 06:00 AM daily
    name="ct_events_schedule",
    execution_timezone="Asia/Dhaka",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "⏰ Runs ct_events_job daily at 06:00 AM (Asia/Dhaka).\n"
        "Scheduled after allsku sync so that mv_allsku lookup data is fresh "
        "before CleverTap event enrichment begins."
    ),
)


# ---------------------------------------------------------------------------
# Definitions  (Dagster entry point)
# ---------------------------------------------------------------------------

defs = Definitions(

    assets=[
        ct_watermarks,
        ct_lookups,
        fe_clevertap,
        fe_clevertap_indexes,
        allsku_extract,
        allsku_load,
    ],
    jobs=[
        # risk_raw_data_job,
        risk_engine_job,
        mkt_com_dump_job,
        mkt_summary_job,
        allsku_job,
        ct_events_job,
    ],
    schedules=[
        # risk_raw_data_schedule,
        risk_engine_schedule,
        mkt_com_dump_schedule,
        mkt_summary_schedule,
        allsku_schedule,
        ct_events_schedule,
    ],
)