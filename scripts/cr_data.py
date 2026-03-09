
import psycopg2
import pandas as pd
import numpy as np 
from datetime import datetime
import os   
import sys
# Add utils folder to path
sys.path.append(r'C://Python_scripts')
import warnings
warnings.filterwarnings("ignore")
import logging
from sqlalchemy import create_engine, text
from utils.log_file import LogManager

# Set up logging
log_manager = LogManager("cr_data")
logger = logging.getLogger(__name__)
logger.info("Starting CR data processing script")

# --- Database configuration ---
logger.info("Configuring database connection")
db_config = {
    "dbname": "report_service",
    "user": "anonna_sll_88437_cartup",
    "password": "#ehrnFhsb&52b%J2K#",
    "host": "172.31.77.37",
    "port": "5432"
}

# --- Connect to database ---
try:
    conn = psycopg2.connect(**db_config)
    logger.info("Successfully connected to the database")
except Exception as e:
    logger.error("Database connection failed: %s", str(e))
    exit()

logger.info("Loading SQL query from file")
with open('C://Python_scripts//query//cr.sql', 'r', encoding='utf-8') as file:
    sql_query = file.read()
    
logger.info("Executing SQL query to fetch CR data")
df_sql = pd.read_sql(sql_query, conn)
logger.info("Successfully fetched %d records from database", len(df_sql))

# Define the directory
folder_path = r'C://Users//metabase//OneDrive - Shoplover Ltd//3pl_partner//'
logger.info("Searching for 3PL files in: %s", folder_path)

# Find all CSV files in the folder
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv') and 'ops_3pl_file' in f]

if not csv_files:
    logger.warning("No file named 'ops_3pl_file.csv' found in '%s'", folder_path)
else:
    # Use the first matching file
    csv_file = os.path.join(folder_path, csv_files[0])
    logger.info("Reading data from: %s", csv_file)

    try:
        # Read CSV with only required columns
        df_excel = pd.read_csv(csv_file, usecols=["package_code", "partner_name", "booking_date"])
        logger.info("Successfully loaded CSV data with %d records", len(df_excel))

        # Convert all column names to snake_case
        df_excel.columns = (
            df_excel.columns
            .str.replace(" ", "_")
            .str.replace("/", "_")
            .str.lower()
        )

        logger.debug("First 2 rows of loaded data:\n%s", df_excel.head(2))

    except Exception as e:
        logger.error("Error reading CSV file: %s", str(e))
        raise

# Merge with Excel data on PackageCode
logger.info("Merging SQL and CSV data")
df_final = pd.merge(df_sql, df_excel[['package_code', 'partner_name', 'booking_date']],on='package_code',how='left')
logger.info("Merged dataset contains %d records", len(df_final))

datetime_cols = [
    "order_created_at",
    "order_updated_at",
    "cr_initiated",
    "cr_confirm",
    "crm_rejected",
    "cr_waiting_for_pickup",
    "cr_in_pickup",
    "first_return_pickup_attempt_failed",
    "second_return_pickup_attempt_failed",
    "cr_pickup_successful",
    "return_pickup_canceled",
    "cr_droped_off",
    "in_transit_to_fm_qcc",
    "in_transit_to_fmqcc",
    "pending_at_fmqcc",
    "in_qc",
    "qc_passed",
    "qc_rejected",
    "qcc_mu_packed",
    "waiting_for_rtm",
    "waiting_for_rtc",
    "in_transit_to_fmr",
    "pending_at_fmr",
    "fmr_mu_pack_created",
    "in_transit_to_fm",
    "in_transit",
    "pending_at_fm",
    "in_seller_delivery",
    "cr_returned_to_seller",
    "pending_at_lm",
    "delivered",
    "return_successful",
    "booking_date",
]

# Loop through each datetime column
logger.info("Processing datetime columns")
for col in datetime_cols:
    if col in df_final.columns:
        # Save original blank positions
        original_blanks = df_final[col].isna() | (df_final[col] == "")
        
        # Convert to datetime, coerce errors to NaT
        df_final[col] = pd.to_datetime(df_final[col], errors="coerce")
        
        # Replace NaT (invalid/coerced dates) back to blank string ''
        df_final.loc[original_blanks, col] = ""
logger.info("Completed datetime column processing")

# Function to calculate time differences in hours
def calculate_time_diff_hours(start_time, end_time):
    return (end_time - start_time).dt.total_seconds() / 3600

# Function to calculate time differences in days
def calculate_time_diff_days(start_time, end_time):
    return (end_time - start_time).dt.days

# Get today's date (date only, no time)
logger.info("Calculating time-based metrics")
today = pd.Timestamp.today().normalize()

# Creating cr_initiated_week
df_final["cr_initiated_week"] = df_final["cr_initiated"].dt.strftime("%U").astype(int) + 1

# Calculate various time differences in hours
logger.info("Computing time differences between statuses")
df_final["cr_initiated_to_cr_confirm"] = calculate_time_diff_hours(df_final["cr_initiated"], df_final["cr_confirm"])
df_final["cr_pickup_to_confirm"] = calculate_time_diff_hours(df_final["cr_confirm"], df_final["cr_pickup_successful"])
df_final["cr_pickup_successful_to_in_transit_to_fmqcc"] = calculate_time_diff_hours(df_final["cr_pickup_successful"], df_final["in_transit_to_fmqcc"])
df_final["in_transit_to_fmqcc_to_pending_at_fmqcc"] = calculate_time_diff_hours(df_final["in_transit_to_fmqcc"], df_final["pending_at_fmqcc"])
df_final["pending_at_fmqcc_to_qc_passed"] = calculate_time_diff_hours(df_final["pending_at_fmqcc"], df_final["qc_passed"])

# Calculate booking age in days
df_final["booking_age"] = calculate_time_diff_days(df_final["booking_date"], today)
logger.info("Completed time-based calculations")

# find out the each package latest status
# ---------------------------------------------
# List of relevant timestamp columns (from start to end of the CRM process)
status_hierarchy = [
    'cr_initiated',	
    'cr_confirm', 
    'crm_rejected', 
    'cr_waiting_for_pickup', 
    'cr_in_pickup', 
    'first_return_pickup_attempt_failed', 
    'second_return_pickup_attempt_failed', 
    'cr_pickup_successful', 
    'return_pickup_canceled', 
    'cr_droped_off', 
    'pending_at_lm', 
    'in_transit_to_fm_qcc', 
    'in_transit_to_fmqcc', 
    'pending_at_fmqcc', 
    'in_qc', 
    'qc_passed', 
    'qc_rejected', 
    'waiting_for_rtm', 
    'waiting_for_rtc', 
    'qcc_mu_packed', 
    'in_transit_to_fmr', 
    'fmr_mu_pack_created', 
    'pending_at_fmr',  
    'in_transit_to_fm', 
    'pending_at_fm', 
    'pending_at_lm',  
    'in_seller_delivery', 
    'cr_returned_to_seller', 
    'return_successful' 
]

# Create a dictionary to map status to priority level
hierarchy_priority = {status: idx for idx, status in enumerate(status_hierarchy)}

def get_latest_status(row):
    # Filter non-null timestamps
    valid_timestamps = {
        status: pd.to_datetime(row[status], errors='coerce')
        for status in status_hierarchy
        if pd.notna(row[status])
    }
    if not valid_timestamps:
        return None
    # Get the maximum timestamp
    max_timestamp = max(valid_timestamps.values())
    # Filter statuses with max timestamp
    candidates = [
        status for status, ts in valid_timestamps.items()
        if ts == max_timestamp
    ]
    # Return the status with the highest priority (latest in hierarchy)
    return max(candidates, key=lambda x: hierarchy_priority[x])

# Add latest_status column
logger.info("Processing package status hierarchy")
df_final['latest_status'] = df_final.apply(get_latest_status, axis=1)

# Log status distribution
status_counts = df_final['latest_status'].value_counts()
logger.info("Status distribution:")
for status, count in status_counts.items():
    if pd.notna(status):
        logger.info("  %s: %d packages", status, count)

logger.info("Preparing data for database upload")
# Step 1: Fill missing columns with None (or NaN)
# Ensure df_final exists before defining table columns
if 'df_final' not in locals():
    logger.error("DataFrame 'df_final' not found")
    raise NameError("DataFrame 'df_final' must be created before processing table columns")

# Target table column list in correct order
table_columns = [
    'package_code', 'original_package_code', 'order_return_package_code',
    'order_id', 'customer_order_code', 'order_code', 'order_return_code',
    'seller_id', 'seller_code', 'shop_name', 'fulfillment_by',
    'product_name', 'quantity', 'total_price', 'total_package_weight_in_kg',
    'seller_status', 'return_status', 'order_created_at', 'order_updated_at',
    'delivered', 'cr_initiated', 'cr_confirm', 'crm_rejected',
    'cr_waiting_for_pickup', 'cr_in_pickup',
    'first_return_pickup_attempt_failed',
    'second_return_pickup_attempt_failed', 'cr_pickup_successful',
    'return_pickup_canceled', 'cr_droped_off', 'pending_at_lm',
    'in_transit_to_fm_qcc', 'in_transit_to_fmqcc', 'pending_at_fmqcc',
    'in_qc', 'qc_passed', 'qc_rejected', 'waiting_for_rtm', 'waiting_for_rtc',
    'qcc_mu_packed', 'in_transit_to_fmr', 'fmr_mu_pack_created', 'pending_at_fmr',
    'in_transit_to_fm', 'pending_at_fm',
    'in_seller_delivery', 'cr_returned_to_seller', 'return_successful',
    'source_hub', 'destination_hub', 'payment_method', 'return_reason',
    'return_reason_description', 'partner_name', 'booking_date',
    'cr_initiated_week', 'cr_initiated_to_cr_confirm', 'cr_pickup_to_confirm',
    'cr_pickup_successful_to_in_transit_to_fmqcc',
    'in_transit_to_fmqcc_to_pending_at_fmqcc', 'pending_at_fmqcc_to_qc_passed',
    'booking_age', 'latest_status', 'refund_method'
]
for col in table_columns:
    if col not in df_final.columns:
        df_final[col] = None
logger.debug("Added missing columns with null values")

# Step 2: Reorder columns to match the table schema
df_aligned = df_final[table_columns]
logger.info("Data aligned with target schema: %d columns", len(table_columns))

# Create engine and upload to database
logger.info("Creating database engine for final upload")
engine = create_engine('postgresql://postgres:admin123@172.16.90.18:5432/postgres')

# Truncate the table
logger.info("Truncating existing table")
try:
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE cr_package_journey"))
        conn.commit()
    logger.info("Table truncated successfully")
except Exception as e:
    logger.error("Error truncating table: %s", str(e))
    raise

# Upload DataFrame to Postgres
logger.info("Uploading data to cr_package_journey table")
try:
    df_aligned.to_sql('cr_package_journey', engine, if_exists='append', index=False)
    logger.info("Successfully uploaded %d records to database", len(df_aligned))
except Exception as e:
    logger.error("Error uploading data to database: %s", str(e))
    raise

logger.info("CR data processing completed successfully")
print("CR Data updated")
